"""
Ripgrep-based Excel Search

Ultra-fast content search using ripgrep on converted CSV files.
10-100x faster than pandas for large Excel files.

Features:
- Full-text search across all sheets
- Pattern matching with regex
- Case-sensitive/insensitive search
- Returns Excel coordinates for all matches
"""

import subprocess
import json
import re
import shutil
from pathlib import Path
from typing import List, Dict, Optional, Union, Literal
from dataclasses import dataclass

from config.settings import settings
from src.utils.logger import logger, LogContext, log_search_results
from src.preprocessing.csv_converter import CSVConverter, CSVParser, convert_excel_to_csv
from src.cache.cache_manager import get_cache_manager, CacheKey
from src.utils.coordinate_mapper import CoordinateMapper


@dataclass
class SearchMatch:
    """Represents a single search match"""
    excel_file: str
    sheet_name: str
    cell_ref: str          # A1 notation
    row: int               # Excel row (1-based)
    col: int               # Excel column (1-based)
    value: str             # Cell value
    matched_text: str      # The text that matched
    context: str           # Surrounding context
    line_number: int       # Line in CSV file
    confidence: float      # Match confidence (0-1)


class RipgrepSearcher:
    """
    Fast content search using ripgrep
    
    Searches across converted CSV files and maps results back to Excel coordinates
    """
    
    def __init__(self):
        self.cache = get_cache_manager()
        self.csv_converter = CSVConverter()
        
        # Verify ripgrep is available
        if settings.enable_ripgrep and not self._check_ripgrep():
            logger.warning("Ripgrep not found, search will be slower")
            settings.enable_ripgrep = False
    
    def search(self,
               query: str,
               excel_files: List[Union[str, Path]],
               case_sensitive: bool = False,
               use_regex: bool = False,
               max_results: Optional[int] = None) -> List[SearchMatch]:
        """
        Search for query across Excel files
        
        Args:
            query: Search query (text or regex pattern)
            excel_files: List of Excel file paths
            case_sensitive: If True, case-sensitive search
            use_regex: If True, treat query as regex pattern
            max_results: Maximum number of results to return
            
        Returns:
            List of SearchMatch objects
        """
        max_results = max_results or settings.max_search_results
        
        with LogContext("ripgrep_search", query=query, files=len(excel_files)):
            # Convert Excel files to CSV
            csv_files_map = self._prepare_csv_files(excel_files)
            
            if not csv_files_map:
                logger.warning("No CSV files prepared for search")
                return []
            
            # Perform ripgrep search
            if settings.enable_ripgrep:
                raw_matches = self._search_with_ripgrep(
                    query,
                    csv_files_map,
                    case_sensitive,
                    use_regex
                )
            else:
                # Fallback to Python search
                raw_matches = self._search_with_python(
                    query,
                    csv_files_map,
                    case_sensitive
                )
            
            # Parse and format results
            matches = self._parse_matches(raw_matches, csv_files_map)
            
            # Sort by confidence
            matches.sort(key=lambda m: m.confidence, reverse=True)
            
            # Limit results
            if len(matches) > max_results:
                matches = matches[:max_results]
            
            log_search_results(
                query,
                len(matches),
                0,  # Duration calculated by LogContext
                "ripgrep" if settings.enable_ripgrep else "python"
            )
            
            return matches
    
    def _prepare_csv_files(self, 
                          excel_files: List[Union[str, Path]]) -> Dict[Path, Dict[str, Path]]:
        """
        Convert Excel files to CSV format
        
        Args:
            excel_files: List of Excel files
            
        Returns:
            Dict mapping Excel path -> {sheet_name -> CSV path}
        """
        csv_files_map = {}
        
        for excel_file in excel_files:
            excel_path = Path(excel_file)
            
            if not excel_path.exists():
                logger.warning(f"Excel file not found: {excel_path}")
                continue
            
            try:
                # Convert to CSV (uses cache if available)
                csv_files = convert_excel_to_csv(excel_path, use_cache=True)
                csv_files_map[excel_path] = csv_files
                
            except Exception as e:
                logger.error(f"Error converting {excel_path.name}: {e}")
                continue
        
        return csv_files_map
    
    def _search_with_ripgrep(self,
                            query: str,
                            csv_files_map: Dict[Path, Dict[str, Path]],
                            case_sensitive: bool,
                            use_regex: bool) -> List[Dict]:
        """
        Search using ripgrep (fast)
        
        Args:
            query: Search query
            csv_files_map: Map of Excel -> CSV files
            case_sensitive: Case sensitivity
            use_regex: Use regex matching
            
        Returns:
            List of raw match dictionaries
        """
        # Collect all CSV file paths
        all_csv_files = []
        for csv_files in csv_files_map.values():
            all_csv_files.extend(csv_files.values())
        
        if not all_csv_files:
            return []
        
        # Build ripgrep command
        rg_args = [
            settings.ripgrep_binary,
            '--json',              # JSON output for parsing
            '--line-number',       # Include line numbers
            '--no-heading',        # No file headers
            '--max-count', str(settings.max_search_results),  # Limit per file
        ]
        
        # Case sensitivity
        if not case_sensitive:
            rg_args.append('--ignore-case')
        
        # Regex vs literal
        if not use_regex:
            rg_args.append('--fixed-strings')  # Literal string search
        
        # Add query
        rg_args.append(query)
        
        # Add all CSV files
        rg_args.extend([str(f) for f in all_csv_files])
        
        try:
            # Execute ripgrep
            logger.debug(f"Executing ripgrep: {' '.join(rg_args[:5])}...")
            
            result = subprocess.run(
                rg_args,
                capture_output=True,
                text=True,
                timeout=settings.ripgrep_timeout
            )
            
            # Parse JSON output
            matches = []
            for line in result.stdout.strip().split('\n'):
                if not line:
                    continue
                
                try:
                    match_data = json.loads(line)
                    if match_data.get('type') == 'match':
                        matches.append(match_data)
                except json.JSONDecodeError:
                    continue
            
            logger.debug(f"Ripgrep found {len(matches)} raw matches")
            return matches
            
        except subprocess.TimeoutExpired:
            logger.error(f"Ripgrep timeout after {settings.ripgrep_timeout}s")
            return []
        except Exception as e:
            logger.error(f"Ripgrep error: {e}")
            return []
    
    def _search_with_python(self,
                           query: str,
                           csv_files_map: Dict[Path, Dict[str, Path]],
                           case_sensitive: bool) -> List[Dict]:
        """
        Fallback search using Python (slower but always available)
        
        Args:
            query: Search query
            csv_files_map: Map of Excel -> CSV files
            case_sensitive: Case sensitivity
            
        Returns:
            List of raw match dictionaries (compatible with ripgrep format)
        """
        matches = []
        query_lower = query if case_sensitive else query.lower()
        
        for excel_path, csv_files in csv_files_map.items():
            for sheet_name, csv_path in csv_files.items():
                
                try:
                    with open(csv_path, 'r', encoding='utf-8') as f:
                        for line_num, line in enumerate(f, 1):
                            # Check if query is in line
                            line_compare = line if case_sensitive else line.lower()
                            
                            if query_lower in line_compare:
                                # Create match dict compatible with ripgrep format
                                matches.append({
                                    'type': 'match',
                                    'data': {
                                        'path': {'text': str(csv_path)},
                                        'line_number': line_num,
                                        'lines': {'text': line.rstrip()},
                                        'submatches': [{
                                            'match': {'text': query}
                                        }]
                                    }
                                })
                                
                                # Limit results
                                if len(matches) >= settings.max_search_results:
                                    return matches
                
                except Exception as e:
                    logger.warning(f"Error searching {csv_path.name}: {e}")
                    continue
        
        logger.debug(f"Python search found {len(matches)} matches")
        return matches
    
    def _parse_matches(self,
                      raw_matches: List[Dict],
                      csv_files_map: Dict[Path, Dict[str, Path]]) -> List[SearchMatch]:
        """
        Parse raw ripgrep matches to SearchMatch objects
        
        Args:
            raw_matches: Raw match data from ripgrep
            csv_files_map: Map of Excel -> CSV files
            
        Returns:
            List of SearchMatch objects
        """
        search_matches = []
        
        # Create reverse mapping: CSV path -> (Excel path, sheet name)
        csv_to_excel = {}
        for excel_path, csv_files in csv_files_map.items():
            for sheet_name, csv_path in csv_files.items():
                csv_to_excel[str(csv_path)] = (excel_path, sheet_name)
        
        for raw_match in raw_matches:
            try:
                match_data = raw_match['data']
                csv_path = match_data['path']['text']
                line_number = match_data['line_number']
                line_text = match_data['lines']['text']
                
                # Get Excel file and sheet name
                if csv_path not in csv_to_excel:
                    continue
                
                excel_path, sheet_name = csv_to_excel[csv_path]
                
                # Extract matched text
                submatches = match_data.get('submatches', [])
                matched_text = submatches[0]['match']['text'] if submatches else ""
                
                # Parse cells in the line to find the match
                cells = line_text.split('\t')
                
                for cell_str in cells:
                    # Check if this cell contains the match
                    if matched_text.lower() in cell_str.lower():
                        parsed_cell = CSVParser.parse_cell(cell_str)
                        
                        if parsed_cell['row'] is not None:
                            # Create SearchMatch
                            match = SearchMatch(
                                excel_file=str(excel_path),
                                sheet_name=sheet_name,
                                cell_ref=parsed_cell['cell_ref'],
                                row=parsed_cell['row'],
                                col=parsed_cell['col'],
                                value=parsed_cell['value'],
                                matched_text=matched_text,
                                context=self._get_context(cells, cell_str),
                                line_number=line_number,
                                confidence=self._calculate_confidence(
                                    matched_text,
                                    parsed_cell['value']
                                )
                            )
                            
                            search_matches.append(match)
            
            except Exception as e:
                logger.debug(f"Error parsing match: {e}")
                continue
        
        return search_matches
    
    def _get_context(self, cells: List[str], matched_cell: str) -> str:
        """
        Get surrounding context for a matched cell
        
        Args:
            cells: All cells in the row
            matched_cell: The cell that matched
            
        Returns:
            Context string
        """
        try:
            idx = cells.index(matched_cell)
            
            # Get 1 cell before and after
            context_cells = []
            
            if idx > 0:
                prev_cell = CSVParser.parse_cell(cells[idx - 1])
                context_cells.append(prev_cell['value'])
            
            curr_cell = CSVParser.parse_cell(matched_cell)
            context_cells.append(f"[{curr_cell['value']}]")
            
            if idx < len(cells) - 1:
                next_cell = CSVParser.parse_cell(cells[idx + 1])
                context_cells.append(next_cell['value'])
            
            return " | ".join(context_cells)
        
        except Exception:
            return matched_cell
    
    def _calculate_confidence(self, matched_text: str, cell_value: str) -> float:
        """
        Calculate confidence score for a match
        
        Args:
            matched_text: The search query
            cell_value: The cell value
            
        Returns:
            Confidence score 0-1
        """
        if not cell_value:
            return 0.0
        
        # Exact match
        if matched_text.lower() == cell_value.lower():
            return 1.0
        
        # Full word match
        if re.search(r'\b' + re.escape(matched_text.lower()) + r'\b', cell_value.lower()):
            return 0.9
        
        # Substring match - score based on proportion
        proportion = len(matched_text) / len(cell_value)
        return min(0.8, 0.5 + proportion * 0.3)
    
    def _check_ripgrep(self) -> bool:
        """Check if ripgrep is available"""
        return shutil.which(settings.ripgrep_binary) is not None
    
    def search_in_sheet(self,
                       query: str,
                       excel_file: Union[str, Path],
                       sheet_name: str,
                       **kwargs) -> List[SearchMatch]:
        """
        Search within a specific sheet
        
        Args:
            query: Search query
            excel_file: Excel file path
            sheet_name: Sheet name to search in
            **kwargs: Additional search options
            
        Returns:
            List of SearchMatch objects
        """
        # Get all matches
        all_matches = self.search(query, [excel_file], **kwargs)
        
        # Filter by sheet
        sheet_matches = [
            m for m in all_matches 
            if m.sheet_name == sheet_name
        ]
        
        return sheet_matches
    
    def search_in_column(self,
                        query: str,
                        excel_file: Union[str, Path],
                        column: Union[str, int],
                        **kwargs) -> List[SearchMatch]:
        """
        Search within a specific column
        
        Args:
            query: Search query
            excel_file: Excel file path
            column: Column letter (A, B) or index (1, 2)
            **kwargs: Additional search options
            
        Returns:
            List of SearchMatch objects
        """
        # Get all matches
        all_matches = self.search(query, [excel_file], **kwargs)
        
        # Convert column to index if letter
        if isinstance(column, str):
            col_idx = CoordinateMapper.column_letter_to_index(column, zero_based=False)
        else:
            col_idx = column
        
        # Filter by column
        column_matches = [
            m for m in all_matches 
            if m.col == col_idx
        ]
        
        return column_matches
    
    def search_in_range(self,
                       query: str,
                       excel_file: Union[str, Path],
                       sheet_name: str,
                       range_str: str,
                       **kwargs) -> List[SearchMatch]:
        """
        Search within a specific range
        
        Args:
            query: Search query
            excel_file: Excel file path
            sheet_name: Sheet name
            range_str: Range like "A1:C10"
            **kwargs: Additional search options
            
        Returns:
            List of SearchMatch objects
        """
        # Parse range
        (start_row, start_col), (end_row, end_col) = CoordinateMapper.parse_range(range_str)
        
        # Convert to 1-based
        start_row += 1
        start_col += 1
        end_row += 1
        end_col += 1
        
        # Get all matches in sheet
        sheet_matches = self.search_in_sheet(query, excel_file, sheet_name, **kwargs)
        
        # Filter by range
        range_matches = [
            m for m in sheet_matches
            if (start_row <= m.row <= end_row and
                start_col <= m.col <= end_col)
        ]
        
        return range_matches


# Convenience function
def search_excel(query: str,
                excel_files: Union[str, Path, List[Union[str, Path]]],
                **kwargs) -> List[SearchMatch]:
    """
    Quick function to search Excel files
    
    Args:
        query: Search query
        excel_files: Single file or list of files
        **kwargs: Additional search options
        
    Returns:
        List of SearchMatch objects
    """
    if isinstance(excel_files, (str, Path)):
        excel_files = [excel_files]
    
    searcher = RipgrepSearcher()
    return searcher.search(query, excel_files, **kwargs)


# Export main components
__all__ = [
    'RipgrepSearcher',
    'SearchMatch',
    'search_excel',
]


if __name__ == "__main__":
    import sys
    from pprint import pprint
    
    print("=== Ripgrep Searcher Tests ===\n")
    
    # Check if ripgrep is available
    searcher = RipgrepSearcher()
    rg_available = searcher._check_ripgrep()
    print(f"Ripgrep available: {rg_available}")
    
    if not rg_available:
        print("Install ripgrep for better performance:")
        print("  Ubuntu/Debian: sudo apt-get install ripgrep")
        print("  macOS: brew install ripgrep")
        print("  Windows: choco install ripgrep")
        print("\nFalling back to Python search...\n")
    
    # Test with file if provided
    if len(sys.argv) > 2:
        excel_file = sys.argv[1]
        query = sys.argv[2]
        
        print(f"Searching for '{query}' in {excel_file}...\n")
        
        try:
            matches = search_excel(query, excel_file, case_sensitive=False)
            
            print(f"Found {len(matches)} matches:\n")
            
            for i, match in enumerate(matches[:10], 1):
                print(f"{i}. Sheet: {match.sheet_name}, Cell: {match.cell_ref}")
                print(f"   Value: {match.value}")
                print(f"   Confidence: {match.confidence:.2f}")
                print(f"   Context: {match.context[:80]}...")
                print()
            
            if len(matches) > 10:
                print(f"... and {len(matches) - 10} more matches")
        
        except Exception as e:
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()
    
    else:
        print("Usage:")
        print("  python src/search/ripgrep_searcher.py <excel_file> <search_query>")
        print("\nExample:")
        print("  python src/search/ripgrep_searcher.py data.xlsx 'revenue'")
    
    print("\n=== Tests Complete ===")