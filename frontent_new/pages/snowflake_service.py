import os
from typing import List, Dict, Any, Tuple
import uuid
from datetime import datetime
import json
import numpy as np

from snowflake.snowpark import Session
from snowflake.core import Root
from snowflake.cortex import Complete

from trulens.core import TruSession, Feedback, Select
from trulens.apps.custom import instrument, TruCustomApp
from trulens.providers.cortex import Cortex

class SnowflakeService:
    def __init__(self, connection_params: Dict[str, str] = None):
        """Initialize Snowflake service with connection parameters."""
        if connection_params is None:
            # Required environment variables
            required_env_vars = [
                "SNOWFLAKE_ACCOUNT",
                "SNOWFLAKE_USER",
                "SNOWFLAKE_PASSWORD",
                "SNOWFLAKE_ROLE",
                "SNOWFLAKE_WAREHOUSE"
            ]
            # Check for missing environment variables
            missing_vars = [var for var in required_env_vars if not os.getenv(var)]
            if missing_vars:
                raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")

            # Build connection params from environment variables
            connection_params = {
                "account": os.getenv("SNOWFLAKE_ACCOUNT"),
                "user": os.getenv("SNOWFLAKE_USER"),
                "password": os.getenv("SNOWFLAKE_PASSWORD"),
                "role": os.getenv("SNOWFLAKE_ROLE"),
                "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
                "database": os.getenv("SNOWFLAKE_DATABASE", "RAG_CHAT_DB"),
                "schema": os.getenv("SNOWFLAKE_SCHEMA", "RAG_CHAT_SCHEMA")
            }

        self.connection_params = connection_params
        self.session = Session.builder.configs(connection_params).create()
        self.root = Root(self.session)
        self.db_name = connection_params.get('database', 'RAG_CHAT_DB')
        self.schema_name = connection_params.get('schema', 'RAG_CHAT_SCHEMA')
        self.warehouse = connection_params.get('warehouse', 'COMPUTE_WH')
        
        # Initialize TruLens
        self.tru_provider = Cortex(
            snowpark_session=self.session,
            model_engine="mistral-large"  # Or your desired model
        )
        self.tru_session = TruSession()
        
        # Initialize RAG app with TruLens monitoring
        self._setup_rag_app()

    def _setup_rag_app(self):
        """Set up RAG application with TruLens monitoring."""
        # Define feedback functions (example RAG Triad)
        self.f_groundedness = (
            Feedback(self.tru_provider.groundedness_measure_with_cot_reasons, 
                     name="Groundedness")
            .on_input()
            .on_output()
            .aggregate(np.mean)
        )

        self.f_context_relevance = (
            Feedback(self.tru_provider.context_relevance_with_cot_reasons,
                     name="Context Relevance")
            .on_input()
            .on_output()
            .aggregate(np.mean)
        )

        self.f_answer_relevance = (
            Feedback(self.tru_provider.relevance_with_cot_reasons,
                     name="Answer Relevance")
            .on_input()
            .on_output()
            .aggregate(np.mean)
        )

        # Create a TruCustomApp wrapper
        self.tru_rag_app = TruCustomApp(
            app=self,
            app_name="SnowflakeRAG",
            feedbacks=[
                self.f_groundedness,
                self.f_context_relevance,
                self.f_answer_relevance
            ]
        )

    def upload_file(self, file_path: str, file_content: bytes, file_type: str) -> bool:
        """Upload a file to Snowflake stage without compression."""
        # Validate input parameters
        if not file_path or not isinstance(file_path, str):
            raise ValueError("file_path must be a non-empty string")
        if not file_content or not isinstance(file_content, bytes):
            raise ValueError("file_content must be non-empty bytes")
        if not file_type or not isinstance(file_type, str):
            raise ValueError("file_type must be a non-empty string")

        # Validate file type
        file_type = file_type.upper()
        if file_type not in ['PDF', 'MD']:
            raise ValueError("Unsupported file_type. Only 'PDF' and 'MD' are supported.")
        
        # Validate file extension matches file_type
        import os
        ext = os.path.splitext(file_path)[1].lower()
        expected_ext = '.pdf' if file_type == 'PDF' else '.md'
        if ext != expected_ext:
            raise ValueError(f"File extension '{ext}' doesn't match file_type '{file_type}'")

        try:
            # Write the file content temporarily so Snowflake can PUT
            with open(file_path, "wb") as f:
                f.write(file_content)

            stage = f"@{self.db_name}.{self.schema_name}.document_stage"
            put_statement = f"""
                PUT file://{file_path} {stage}
                AUTO_COMPRESS = FALSE
                SOURCE_COMPRESSION = NONE
                OVERWRITE = TRUE
                PARALLEL = 4
            """
            self.session.sql(put_statement).collect()
            return True
        except Exception as e:
            print(f"Error uploading file: {e}")
            return False
        finally:
            # Clean up temp file
            try:
                os.remove(file_path)
            except:
                pass

    def process_staged_files(self) -> List[str]:
        """Process all files in stage and create chunks."""
        try:
            list_stmt = f"LIST @{self.db_name}.{self.schema_name}.document_stage"
            staged_files = self.session.sql(list_stmt).collect()
            print(f"Found {len(staged_files)} files in stage")
            if staged_files:
                print("Sample file info:", staged_files[0].asDict())
            
            processed_files = []
            
            for file_info in staged_files:
                file_dict = file_info.asDict()
                staged_path = file_dict.get('name', '')
                if not staged_path:
                    print(f"Warning: No path found in file info: {file_dict}")
                    continue

                import os
                clean_path = staged_path
                if clean_path.startswith('document_stage/'):
                    clean_path = clean_path[len('document_stage/'):]
                
                file_name = os.path.basename(clean_path)
                print(f"Processing file: {file_name} (Path: {clean_path})")

                # Attempt parsing and inserting into DOCUMENT_CHUNKS
                insert_stmt = f"""
                    INSERT INTO DOCUMENT_CHUNKS (
                        RELATIVE_PATH, FILE_NAME, CHUNK_TEXT, FILE_TYPE, FILE_SIZE
                    )
                    SELECT 
                        '{staged_path}',
                        '{file_name}',
                        func.chunk,
                        CASE 
                            WHEN LOWER('{file_name}') LIKE '%.pdf' THEN 'PDF'
                            WHEN LOWER('{file_name}') LIKE '%.md' THEN 'MD'
                            ELSE 'OTHER'
                        END,
                        {file_dict.get('size', 0)}
                    FROM TABLE(
                        text_chunker(
                            TO_VARCHAR(
                                SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
                                    @{self.db_name}.{self.schema_name}.document_stage,
                                    '{clean_path}',
                                    {{'mode': 'LAYOUT'}}
                                )
                            )
                        )
                    ) AS func;
                """

                # (Optional) Just show parse result for debugging
                parse_doc_stmt = f"""
                    SELECT TO_VARCHAR(
                        SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
                            @{self.db_name}.{self.schema_name}.document_stage,
                            '{clean_path}',
                            {{'mode': 'LAYOUT'}}
                        )
                    );
                """
                print("=============Trying parse statement================")
                print(parse_doc_stmt)

                try:
                    parse_result = self.session.sql(parse_doc_stmt).collect()
                    print("Parse result:", parse_result[0].asDict())
                except Exception as parse_err:
                    print(f"Error parsing file: {parse_err}")

                try:
                    self.session.sql(insert_stmt).collect()
                    processed_files.append(file_name)
                    print(f"Successfully processed {file_name}")
                except Exception as file_error:
                    print(f"Error processing {file_dict.get('name', 'unknown file')}: {str(file_error)}")
                    continue
            
            print(f"Successfully processed {len(processed_files)} files")
            return processed_files
        except Exception as e:
            print(f"Error in process_staged_files: {str(e)}")
            import traceback
            print(f"Traceback: {traceback.format_exc()}")
            return []

    @instrument
    def retrieve_context(self, query: str, limit: int = 3) -> List[Dict[str, Any]]:
        """Retrieve relevant document chunks using Cortex Search."""
        try:
            svc = (
                self.root.databases[self.db_name]
                    .schemas[self.schema_name]
                    .cortex_search_services["RAG_SEARCH_SERVICE"]
            )
            response = svc.search(
                query=query,
                columns=["CHUNK_TEXT", "FILE_NAME", "CHUNK_ID"],
                limit=limit
            )
            results = []
            if response and response.results:
                for res in response.results:
                    results.append({
                        "CHUNK_TEXT": res.get("CHUNK_TEXT", ""),
                        "FILE_NAME": res.get("FILE_NAME", ""),
                        "CHUNK_ID": str(res.get("CHUNK_ID", ""))
                    })
            return results
        except Exception as e:
            print(f"Error retrieving context: {e}")
            return []

    @instrument
    def generate_answer(self, query: str, context: List[Dict[str, Any]]) -> str:
        try:
            context_text = "\n\n".join(
                chunk.get("CHUNK_TEXT", "") for chunk in context
            )
            prompt = f"""
            You are a helpful AI assistant. Answer the question based only on the provided context.
            If the answer cannot be found in the context, say "I don't have enough information to answer this question."

            Context:
            {context_text}

            Question: {query}

            Answer:
            """
            # Use param binding:
            sql = """
            SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large', ?)
            AS ANSWER
            """
            result = self.session.sql(sql, params=[prompt]).collect()
            if result and len(result) > 0:
                # Each row is a Row object
                row = result[0]
                # Access column by name using brackets or attribute:
                answer = row["ANSWER"]
                if answer:
                    return answer

            return "I don't have enough information to answer this question."
        except Exception as e:
            print(f"Error generating answer: {e}")
            return "I apologize, but I encountered an error generating the response."


    def chat(self, query: str, chat_id: str = None) -> Tuple[str, Dict[str, Any]]:
        """Main chat function that combines retrieval and generation with TruLens monitoring."""
        if isinstance(query, tuple):
            # If it's a single-element tuple, just take that element.
            # Otherwise, convert entire tuple to a string.
            if len(query) == 1:
                query = query[0]
            else:
                query = " ".join(str(x) for x in query)
                
        if not chat_id:
            chat_id = str(uuid.uuid4())

        try:
            # The main priority is to get an answer. We'll wrap feedback in a separate try block.
            with self.tru_rag_app as recording:
                context = self.retrieve_context(query)
                answer = self.generate_answer(query, context)

            # Attempt to compute feedback scores
            try:
                # Call each feedback function. If it fails, we handle gracefully.
                groundedness_score = self.f_groundedness(context, answer)
                context_rel_score = self.f_context_relevance(query, context)
                answer_rel_score = self.f_answer_relevance(query, answer)
            except Exception as fb_err:
                print(f"Error computing feedback scores: {fb_err}")
                groundedness_score = 0.0
                context_rel_score = 0.0
                answer_rel_score = 0.0

            # Wrap feedback in a dict, ensuring we handle numeric or tuple returns
            processed_scores = {}
            for metric_name, score_obj in [
                ("groundedness", groundedness_score),
                ("context_relevance", context_rel_score),
                ("answer_relevance", answer_rel_score)
            ]:
                # If the feedback function returns (score, metadata)
                # convert to {score, metadata}. Otherwise, just store a float.
                if isinstance(score_obj, tuple) and len(score_obj) == 2:
                    val, meta = score_obj
                    processed_scores[metric_name] = {
                        "score": float(val),
                        "metadata": meta
                    }
                else:
                    # Best-effort float conversion
                    try:
                        processed_scores[metric_name] = float(score_obj)
                    except:
                        processed_scores[metric_name] = 0.0

            # Build metadata
            metadata = {
                "context": context,
                "feedback_scores": processed_scores
            }

            # Attempt storing chat messages
            try:
                self._store_chat_message(chat_id, "user", query)
                self._store_chat_message(chat_id, "assistant", answer, metadata)
            except Exception as e:
                print(f"Error storing chat history: {e}")

            return answer, metadata

        except Exception as e:
            print(f"Error in chat: {e}")
            return "I apologize, but I encountered an error processing your request.", {}

    def _store_chat_message(self, chat_id: str, role: str, content: str, metadata: Dict = None):
        """
        Store a chat message in the history table, ensuring the JSON for METADATA
        is escaped properly so Snowflake's PARSE_JSON can handle it.
        """
        try:
            # Convert non-string content to string
            if not isinstance(content, str):
                content = str(content)

            # Escape single quotes in the content
            safe_content = content.replace("'", "''")

            # Default to NULL if no metadata
            metadata_str = "NULL"

            if metadata:
                # Convert Python dict to JSON string
                metadata_json = json.dumps(metadata)

                # Escape backslashes for safety, otherwise Snowflake may reject them
                # (Double-escape: \\ => \\\\)
                metadata_json = metadata_json.replace("\\", "\\\\")
                
                # Now escape single quotes for SQL
                metadata_json = metadata_json.replace("'", "''")

                # Build the PARSE_JSON expression
                metadata_str = f"PARSE_JSON('{metadata_json}')"

            insert_stmt = f"""
            INSERT INTO CHAT_HISTORY (CHAT_ID, ROLE, CONTENT, METADATA)
            VALUES (
                '{chat_id}',
                '{role}',
                '{safe_content}',
                {metadata_str}
            )
            """
            self.session.sql(insert_stmt).collect()

        except Exception as e:
            print(f"Error storing chat message: {e}")
            print(f"Insert statement was:\n{insert_stmt}")

    def get_chat_history(self, chat_id: str) -> List[Dict[str, Any]]:
        """Retrieve chat history for a given chat ID."""
        try:
            query = f"""
            SELECT MESSAGE_ID, ROLE, CONTENT, CREATED_AT, METADATA
            FROM CHAT_HISTORY
            WHERE CHAT_ID = '{chat_id}'
            ORDER BY MESSAGE_ID
            """
            result = self.session.sql(query).collect()
            return [row.asDict() for row in result]
        except Exception as e:
            print(f"Error retrieving chat history: {e}")
            return []

if __name__ == "__main__":
    # Example usage:
    connection_params = {
        "account": "blcquff-dcb49840",
        "user": "TAZ16",
        "password": "Srinijani@95",
        "role": "ACCOUNTADMIN",
        "warehouse": "COMPUTE_WH",
        "database": "RAG_CHAT_DB",
        "schema": "RAG_CHAT_SCHEMA"
    }
    service = SnowflakeService(connection_params)
    print("Service initialized successfully!\n")
    
    example_query = "What is the capital of France?",
    answer, meta = service.chat(example_query)
    print("User Query:", example_query)
    print("Assistant Answer:", answer)
    print("Metadata:", meta)


