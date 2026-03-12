"""FBref HTML parser handling commented-out tables and multi-level headers."""

from __future__ import annotations

import pandas as pd
from bs4 import BeautifulSoup, Comment

from sports_pipeline.utils.logging import get_logger

log = get_logger(__name__)


class FbrefParser:
    """Parse FBref HTML pages into DataFrames."""

    @staticmethod
    def parse_table(html: str, table_id: str) -> pd.DataFrame | None:
        """Extract a table by ID, handling FBref's commented-out tables.

        FBref hides some tables inside HTML comments. This method
        first tries to find the table normally, then searches comments.
        """
        soup = BeautifulSoup(html, "lxml")

        # Try direct table first
        table = soup.find("table", id=table_id)

        # If not found, search inside HTML comments
        if table is None:
            for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
                if table_id in comment:
                    comment_soup = BeautifulSoup(comment, "lxml")
                    table = comment_soup.find("table", id=table_id)
                    if table:
                        break

        if table is None:
            log.warning("table_not_found", table_id=table_id)
            return None

        return FbrefParser._table_to_dataframe(table)

    @staticmethod
    def parse_all_tables(html: str) -> dict[str, pd.DataFrame]:
        """Extract all tables from the page."""
        soup = BeautifulSoup(html, "lxml")
        tables = {}

        # Direct tables
        for table in soup.find_all("table"):
            tid = table.get("id")
            if tid:
                tables[tid] = FbrefParser._table_to_dataframe(table)

        # Commented-out tables
        for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
            comment_soup = BeautifulSoup(comment, "lxml")
            for table in comment_soup.find_all("table"):
                tid = table.get("id")
                if tid and tid not in tables:
                    tables[tid] = FbrefParser._table_to_dataframe(table)

        return tables

    @staticmethod
    def _table_to_dataframe(table) -> pd.DataFrame:
        """Convert a BeautifulSoup table to a DataFrame, handling multi-level headers."""
        # Get header rows
        thead = table.find("thead")
        if thead is None:
            # Simple table without thead
            rows = table.find_all("tr")
            if not rows:
                return pd.DataFrame()
            headers = [th.get_text(strip=True) for th in rows[0].find_all(["th", "td"])]
            data_rows = rows[1:]
        else:
            header_rows = thead.find_all("tr")
            # Use last header row as column names (handles multi-level)
            last_header = header_rows[-1]
            headers = [th.get_text(strip=True) for th in last_header.find_all("th")]
            data_rows = table.find("tbody").find_all("tr") if table.find("tbody") else []

        # Parse data rows, skipping spacer rows
        data = []
        for row in data_rows:
            if "spacer" in row.get("class", []) or "thead" in row.get("class", []):
                continue
            cells = row.find_all(["td", "th"])
            data.append([cell.get_text(strip=True) for cell in cells])

        if not data:
            return pd.DataFrame(columns=headers)

        df = pd.DataFrame(data)

        # Align columns
        if len(headers) == df.shape[1]:
            df.columns = headers
        elif len(headers) > 0:
            # Truncate or pad
            padded = headers + [
                f"col_{i}" for i in range(len(headers), df.shape[1])
            ]
            df.columns = padded[:df.shape[1]]

        # Replace empty strings with NaN
        df = df.replace("", pd.NA)

        return df

    @staticmethod
    def extract_match_links(html: str, table_id: str = "sched_all") -> list[str]:
        """Extract match report URLs from a schedule table."""
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table", id=table_id)

        if table is None:
            for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
                if table_id in comment:
                    comment_soup = BeautifulSoup(comment, "lxml")
                    table = comment_soup.find("table", id=table_id)
                    if table:
                        break

        if table is None:
            return []

        links = []
        for a_tag in table.find_all("a", href=True):
            href = a_tag["href"]
            if "/matches/" in href and "Match-Report" in a_tag.get_text():
                links.append(href)

        return links
