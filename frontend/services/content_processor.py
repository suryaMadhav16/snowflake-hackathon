import logging
from typing import List, Dict, Optional
import streamlit as st
from urllib.parse import urlparse
from services.snowflake_client import SnowflakeClient

logger = logging.getLogger(__name__)

class ContentProcessor:
    def __init__(self, snowflake_client: Optional[SnowflakeClient] = None):
        self.snowflake = snowflake_client or SnowflakeClient()

    async def process_crawl_results(self, results: List[Dict]) -> bool:
        """Process crawl results and store in Snowflake"""
        logger.info(f"Starting to process {len(results)} crawl results")
        try:
            success_count = 0
            for index, result in enumerate(results):
                logger.debug(f"Processing result {index + 1}/{len(results)}")
                if not result.get("success"):
                    continue

                success = await self._process_single_result(result)
                if success:
                    success_count += 1

            logger.info(f"Successfully processed {success_count}/{len(results)} results")
            if success_count > 0:
                st.success(f"Successfully processed {success_count} documents for RAG")
            return success_count > 0

        except Exception as e:
            error_msg = f"Error processing results for RAG: {str(e)}"
            logger.error(error_msg, exc_info=True)
            st.error(error_msg)
            return False

    async def _process_single_result(self, result: Dict) -> bool:
        """Process a single crawl result"""
        try:
            file_name = result["url"]
            content = result.get("markdown", "")

            # 1. Insert raw document
            if not self.snowflake.insert_document(file_name, content):
                return False

            # 2. Create chunks
            try:
                self.snowflake.sync_cral_content()
                st.success(f"Successfully processed {file_name}")
            except Exception as e:
                logger.error(f"Error syncing crawl content: {str(e)}", exc_info=True)
                return False
            
            return True

        except Exception as e:
            logger.error(f"Error processing result for {result.get('url')}: {str(e)}", exc_info=True)
            return False

    def cleanup(self):
        """Cleanup resources"""
        try:
            self.snowflake.close()
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}", exc_info=True)
