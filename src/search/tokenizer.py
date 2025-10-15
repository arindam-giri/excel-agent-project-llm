"""
Excel-aware tokenization for search and indexing

Handles special Excel-specific tokenization challenges:
- Column headers (CamelCase, snake_case, spaces, special chars)
- Cell content (mixed formats, numbers, dates)
- User queries (natural language)
- Business terminology and domain-specific terms
"""

import re
import unicodedata
from typing import List, Set, Dict, Optional, Literal
from config.settings import settings
from src.utils.logger import logger


class ExcelTokenizer:
    """
    Advanced tokenizer optimized for Excel data
    
    Handles multiple tokenization strategies based on context
    """
    
    def __init__(self):
        # Business domain stopwords (less aggressive than NLP stopwords)
        self.stopwords = {
            'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
            'of', 'with', 'by', 'from', 'as', 'is', 'was', 'are', 'were', 'been',
            'be', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would',
            'should', 'could', 'may', 'might', 'must', 'can', 'this', 'that',
            'these', 'those', 'what', 'which', 'who', 'when', 'where', 'why', 'how'
        }
        
        # Preserve these even though they look like stopwords
        self.preserve_terms = {
            'total', 'sum', 'net', 'gross', 'average', 'count', 'max', 'min',
            'year', 'month', 'quarter', 'date', 'amount', 'value', 'number',
            'id', 'no', 'vs', 'to', 'from', 'in', 'on', 'at', 'by'
        }
        
        # Common Excel abbreviations
        self.abbreviations = {
            'qty': 'quantity',
            'amt': 'amount',
            'avg': 'average',
            'yr': 'year',
            'mo': 'month',
            'no': 'number',
            'id': 'identifier',
            'desc': 'description',
            'addr': 'address',
            'dept': 'department',
            'mgr': 'manager',
            'emp': 'employee',
            'cust': 'customer',
            'prod': 'product',
            'rev': 'revenue',
            'exp': 'expense',
        }
        
        # Load domain synonyms
        self.domain_synonyms = self._load_domain_synonyms()
    
    def tokenize(self, text: str, 
                 context: Literal['header', 'content', 'query', 'general'] = 'general',
                 expand_synonyms: bool = False) -> List[str]:
        """
        Main tokenization method with context awareness
        
        Args:
            text: Text to tokenize
            context: Context type - determines tokenization strategy
            expand_synonyms: If True, add synonym tokens
            
        Returns:
            List of tokens
        """
        if not text or not isinstance(text, str):
            return []
        
        # Step 1: Normalize
        text = self._normalize(text)
        
        # Step 2: Context-specific tokenization
        if context == 'header':
            tokens = self._tokenize_header(text)
        elif context == 'query':
            tokens = self._tokenize_query(text)
        elif context == 'content':
            tokens = self._tokenize_content(text)
        else:
            tokens = self._tokenize_general(text)
        
        # Step 3: Post-process
        tokens = self._post_process(tokens)
        
        # Step 4: Expand with synonyms if requested
        if expand_synonyms:
            tokens = self._expand_with_synonyms(tokens)
        
        return tokens
    
    def _normalize(self, text: str) -> str:
        """Normalize text for consistent tokenization"""
        # Convert to lowercase
        text = text.lower()
        
        # Remove accents/diacritics
        text = unicodedata.normalize('NFKD', text)
        text = ''.join([c for c in text if not unicodedata.combining(c)])
        
        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove leading/trailing whitespace
        return text.strip()
    
    def _tokenize_header(self, text: str) -> List[str]:
        """
        Tokenize column headers with special handling
        
        Examples:
        - "TotalRevenue" -> ["total", "revenue", "totalrevenue"]
        - "customer_name" -> ["customer", "name", "customer_name"]
        - "Q1 2024 Sales" -> ["q1", "2024", "sales", "q1_2024_sales"]
        """
        tokens = set()
        
        # Keep original (normalized, with underscores for spaces)
        original = re.sub(r'[^\w\s]', '_', text)
        original = re.sub(r'\s+', '_', original)
        if len(original) > settings.min_token_length:
            tokens.add(original)
        
        # Split by common separators
        parts = re.split(r'[_\s\-./]+', text)
        for part in parts:
            if len(part) > settings.min_token_length:
                tokens.add(part)
        
        # Split CamelCase
        camel_split = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)
        camel_parts = camel_split.split()
        for part in camel_parts:
            part_lower = part.lower()
            if len(part_lower) > settings.min_token_length:
                tokens.add(part_lower)
        
        # Extract acronyms (all caps sequences)
        acronyms = re.findall(r'\b[A-Z]{2,}\b', text.upper())
        for acronym in acronyms:
            tokens.add(acronym.lower())
        
        # Create compound tokens (bigrams for multi-word headers)
        parts_list = [p for p in parts if len(p) > settings.min_token_length]
        for i in range(len(parts_list) - 1):
            compound = f"{parts_list[i]}_{parts_list[i+1]}"
            tokens.add(compound)
        
        # Extract numbers separately
        numbers = re.findall(r'\d+', text)
        tokens.update(numbers)
        
        # Extract quarters (Q1, Q2, etc.)
        quarters = re.findall(r'q[1-4]', text.lower())
        tokens.update(quarters)
        
        return list(tokens)
    
    def _tokenize_query(self, text: str) -> List[str]:
        """
        Tokenize user queries - more lenient, preserves intent
        
        Examples:
        - "What is the total revenue?" -> ["total", "revenue"]
        - "Show me Q1 sales data" -> ["q1", "sales", "data"]
        """
        tokens = set()
        
        # Extract quoted phrases as single tokens
        quoted_phrases = re.findall(r'"([^"]+)"', text)
        for phrase in quoted_phrases:
            normalized_phrase = re.sub(r'\s+', '_', phrase.lower())
            tokens.add(normalized_phrase)
            # Remove quoted text from main text
            text = text.replace(f'"{phrase}"', '')
        
        # Basic word tokenization
        words = re.findall(r'\b\w+\b', text)
        
        for word in words:
            word_lower = word.lower()
            
            # Skip stopwords unless preserved
            if word_lower in self.stopwords and word_lower not in self.preserve_terms:
                continue
            
            # Keep words longer than min length
            if len(word_lower) > settings.min_token_length:
                tokens.add(word_lower)
                
                # Expand abbreviations
                if word_lower in self.abbreviations:
                    tokens.add(self.abbreviations[word_lower])
            
            # Keep important short words
            elif word_lower in self.preserve_terms:
                tokens.add(word_lower)
        
        # Extract years (4 digits)
        years = re.findall(r'\b(19|20)\d{2}\b', text)
        tokens.update(years)
        
        # Extract quarters
        quarters = re.findall(r'\bq[1-4]\b', text.lower())
        tokens.update(quarters)
        
        # Extract dates in various formats
        dates = re.findall(r'\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b', text)
        tokens.update(dates)
        
        # Create meaningful bigrams
        words_list = [w for w in words if len(w) > settings.min_token_length]
        for i in range(len(words_list) - 1):
            w1, w2 = words_list[i].lower(), words_list[i+1].lower()
            # Only add if neither is a stopword
            if (w1 not in self.stopwords or w1 in self.preserve_terms) and \
               (w2 not in self.stopwords or w2 in self.preserve_terms):
                bigram = f"{w1}_{w2}"
                tokens.add(bigram)
        
        return list(tokens)
    
    def _tokenize_content(self, text: str) -> List[str]:
        """
        Tokenize cell content - handles mixed formats
        """
        tokens = set()
        
        # Split on any non-alphanumeric
        parts = re.split(r'[^\w]+', text)
        
        for part in parts:
            if len(part) > settings.min_token_length:
                tokens.add(part.lower())
            
            # Extract sub-tokens from camelCase
            if any(c.isupper() for c in part):
                camel_tokens = re.findall(r'[A-Z][a-z]+|[a-z]+', part)
                for token in camel_tokens:
                    if len(token) > settings.min_token_length:
                        tokens.add(token.lower())
        
        # Extract numbers
        numbers = re.findall(r'\d+', text)
        tokens.update(numbers)
        
        # Extract currency amounts
        currency = re.findall(r'[$€£¥]\s*[\d,]+(?:\.\d{2})?', text)
        for curr in currency:
            # Extract just the number
            num = re.sub(r'[^\d.]', '', curr)
            tokens.add(num)
        
        return list(tokens)
    
    def _tokenize_general(self, text: str) -> List[str]:
        """
        General tokenization - balanced approach
        """
        tokens = set()
        
        # Split on whitespace and common punctuation
        parts = re.split(r'[\s,;:.!?]+', text)
        
        for part in parts:
            # Clean the part
            cleaned = re.sub(r'[^\w]', '', part)
            if len(cleaned) > settings.min_token_length:
                tokens.add(cleaned.lower())
        
        return list(tokens)
    
    def _post_process(self, tokens: List[str]) -> List[str]:
        """
        Clean up tokenization results
        """
        processed = []
        
        for token in tokens:
            # Skip empty or very short tokens (except preserved ones)
            if len(token) < settings.min_token_length and token not in self.preserve_terms:
                continue
            
            # Skip pure punctuation
            if re.match(r'^[^\w]+', token):
                continue
            
            # Skip tokens that are all underscores
            if re.match(r'^_+', token):
                continue
            
            # Skip tokens that are only digits if too short
            if token.isdigit() and len(token) == 1:
                continue
            
            processed.append(token)
        
        # Remove duplicates while preserving order
        seen = set()
        result = []
        for token in processed:
            if token not in seen:
                seen.add(token)
                result.append(token)
        
        return result
    
    def _expand_with_synonyms(self, tokens: List[str]) -> List[str]:
        """
        Expand tokens with domain-specific synonyms
        """
        expanded = set(tokens)
        
        for token in tokens:
            if token in self.domain_synonyms:
                expanded.update(self.domain_synonyms[token])
        
        return list(expanded)
    
    def _load_domain_synonyms(self) -> Dict[str, Set[str]]:
        """
        Load domain-specific synonym mappings
        """
        return {
            # Revenue related
            'revenue': {'sales', 'income', 'earnings', 'turnover', 'proceeds'},
            'sales': {'revenue', 'income', 'turnover'},
            'income': {'revenue', 'earnings', 'profit'},
            
            # Cost related
            'cost': {'expense', 'expenditure', 'spending', 'outlay', 'charge'},
            'expense': {'cost', 'expenditure', 'spending'},
            'price': {'cost', 'rate', 'charge', 'fee', 'value'},
            
            # Profit related
            'profit': {'margin', 'earnings', 'gain', 'net_income'},
            'margin': {'profit', 'markup'},
            
            # Customer related
            'customer': {'client', 'buyer', 'account', 'purchaser', 'consumer'},
            'client': {'customer', 'account'},
            
            # Product related
            'product': {'item', 'sku', 'article', 'goods', 'merchandise'},
            'item': {'product', 'sku'},
            
            # Quantity related
            'quantity': {'qty', 'count', 'number', 'volume', 'amount'},
            'qty': {'quantity', 'count', 'number'},
            'count': {'quantity', 'number', 'total'},
            
            # Time related
            'date': {'time', 'period', 'timestamp', 'when'},
            'year': {'yr', 'annual', 'yearly'},
            'month': {'monthly', 'mo'},
            'quarter': {'q1', 'q2', 'q3', 'q4', 'quarterly'},
            
            # Aggregation related
            'total': {'sum', 'aggregate', 'combined', 'overall'},
            'sum': {'total', 'aggregate'},
            'average': {'mean', 'avg'},
            'avg': {'average', 'mean'},
            
            # Financial terms
            'assets': {'holdings', 'resources', 'property'},
            'liabilities': {'debt', 'obligations', 'payables'},
            'equity': {'capital', 'ownership', 'net_worth'},
            'depreciation': {'amortization', 'writedown'},
        }
    
    def create_ngrams(self, tokens: List[str], n: int = 2) -> List[str]:
        """
        Create n-grams from token list
        
        Args:
            tokens: List of tokens
            n: N-gram size
            
        Returns:
            List of n-grams
        """
        ngrams = []
        for i in range(len(tokens) - n + 1):
            ngram = '_'.join(tokens[i:i+n])
            ngrams.append(ngram)
        return ngrams
    
    def extract_entities(self, text: str) -> Dict[str, List[str]]:
        """
        Extract named entities from text
        
        Returns:
            Dict with entity types as keys and lists of entities as values
        """
        entities = {
            'dates': [],
            'numbers': [],
            'currencies': [],
            'codes': [],
            'emails': [],
            'urls': [],
        }
        
        # Dates (various formats)
        date_patterns = [
            r'\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b',  # MM/DD/YYYY, DD-MM-YYYY
            r'\b\d{4}[/-]\d{1,2}[/-]\d{1,2}\b',    # YYYY-MM-DD
            r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},?\s+\d{4}\b',  # Month DD, YYYY
        ]
        for pattern in date_patterns:
            entities['dates'].extend(re.findall(pattern, text, re.IGNORECASE))
        
        # Numbers (including decimals and with commas)
        numbers = re.findall(r'\b\d{1,3}(?:,\d{3})*(?:\.\d+)?\b', text)
        entities['numbers'].extend(numbers)
        
        # Currency amounts
        currencies = re.findall(r'[$€£¥]\s*[\d,]+(?:\.\d{2})?', text)
        entities['currencies'].extend(currencies)
        
        # IDs/Codes (alphanumeric with specific patterns)
        code_patterns = [
            r'\b[A-Z]{2,}\d{3,}\b',  # ABC123
            r'\b\d{3,}-[A-Z0-9]+\b',  # 123-ABC
            r'\b[A-Z]\d{6,}\b',       # A123456
        ]
        for pattern in code_patterns:
            entities['codes'].extend(re.findall(pattern, text))
        
        # Emails
        emails = re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', text)
        entities['emails'].extend(emails)
        
        # URLs
        urls = re.findall(r'https?://[^\s]+', text)
        entities['urls'].extend(urls)
        
        return entities


class FormulaTokenizer:
    """Specialized tokenizer for Excel formulas"""
    
    @staticmethod
    def tokenize_formula(formula: str) -> Dict[str, List[str]]:
        """
        Extract meaningful tokens from Excel formulas
        
        Args:
            formula: Excel formula string
            
        Returns:
            Dict with different token types
        """
        tokens = {
            'functions': [],
            'sheets': [],
            'cell_refs': [],
            'ranges': [],
            'operators': [],
        }
        
        # Remove = sign
        formula = formula.lstrip('=')
        
        # Extract function names
        functions = re.findall(r'\b[A-Z]+(?=\()', formula)
        tokens['functions'] = [f.lower() for f in functions]
        
        # Extract sheet references
        sheet_refs = re.findall(r"(['\"]?[\w\s]+['\"]?)!", formula)
        tokens['sheets'] = [s.strip("'\"") for s in sheet_refs]
        
        # Extract cell references
        cells = re.findall(r'\b[A-Z]+\d+\b', formula)
        tokens['cell_refs'] = [c.lower() for c in cells]
        
        # Extract ranges
        ranges = re.findall(r'[A-Z]+\d+:[A-Z]+\d+', formula)
        tokens['ranges'] = [r.lower() for r in ranges]
        
        # Extract operators
        operators = re.findall(r'[+\-*/^<>=&]', formula)
        tokens['operators'] = operators
        
        return tokens


class PivotFieldTokenizer:
    """Specialized tokenizer for pivot table fields"""
    
    @staticmethod
    def tokenize_pivot_field(field_name: str) -> List[str]:
        """
        Tokenize pivot table field names
        
        Pivot fields often have prefixes like "Sum of", "Count of", etc.
        
        Args:
            field_name: Pivot field name
            
        Returns:
            List of tokens
        """
        tokenizer = ExcelTokenizer()
        tokens = set()
        
        # Remove aggregation prefixes
        cleaned = re.sub(
            r'^(sum|count|average|avg|max|min|stdev|var)\s+of\s+',
            '',
            field_name,
            flags=re.IGNORECASE
        )
        
        # Tokenize the remaining part
        tokens.update(tokenizer.tokenize(cleaned, context='header'))
        
        # Also tokenize original (to catch cases where prefix is meaningful)
        tokens.update(tokenizer.tokenize(field_name, context='header'))
        
        return list(tokens)


# Convenience functions
def tokenize_header(text: str, expand_synonyms: bool = False) -> List[str]:
    """Quick function to tokenize column headers"""
    tokenizer = ExcelTokenizer()
    return tokenizer.tokenize(text, context='header', expand_synonyms=expand_synonyms)


def tokenize_query(text: str, expand_synonyms: bool = True) -> List[str]:
    """Quick function to tokenize user queries"""
    tokenizer = ExcelTokenizer()
    return tokenizer.tokenize(text, context='query', expand_synonyms=expand_synonyms)


def tokenize_content(text: str) -> List[str]:
    """Quick function to tokenize cell content"""
    tokenizer = ExcelTokenizer()
    return tokenizer.tokenize(text, context='content')


# Export main classes and functions
__all__ = [
    'ExcelTokenizer',
    'FormulaTokenizer',
    'PivotFieldTokenizer',
    'tokenize_header',
    'tokenize_query',
    'tokenize_content',
]


if __name__ == "__main__":
    # Test tokenization
    print("=== Excel Tokenizer Tests ===\n")
    
    tokenizer = ExcelTokenizer()
    
    # Test header tokenization
    headers = [
        "TotalRevenue",
        "customer_name",
        "Q1 2024 Sales",
        "Avg. Revenue ($)",
        "YTD_Sales_Amount"
    ]
    
    print("Header Tokenization:")
    for header in headers:
        tokens = tokenizer.tokenize(header, context='header')
        print(f"  '{header}' -> {tokens}")
    
    # Test query tokenization
    queries = [
        "What is the total revenue?",
        "Show me Q1 sales data",
        "Find customer named John",
        "Compare 2023 vs 2024 revenue"
    ]
    
    print("\nQuery Tokenization:")
    for query in queries:
        tokens = tokenizer.tokenize(query, context='query')
        print(f"  '{query}' -> {tokens}")
    
    # Test with synonym expansion
    print("\nQuery with Synonym Expansion:")
    query = "total revenue"
    tokens = tokenizer.tokenize(query, context='query', expand_synonyms=True)
    print(f"  '{query}' -> {tokens}")
    
    # Test entity extraction
    print("\nEntity Extraction:")
    text = "Sales of $1,234.56 on 01/15/2024 by customer ABC123"
    entities = tokenizer.extract_entities(text)
    print(f"  Text: '{text}'")
    print(f"  Entities: {entities}")
    
    # Test formula tokenization
    print("\nFormula Tokenization:")
    formula = "=SUM(Sales!A2:A100)+AVERAGE(B:B)"
    formula_tokens = FormulaTokenizer.tokenize_formula(formula)
    print(f"  Formula: '{formula}'")
    print(f"  Tokens: {formula_tokens}")
    
    # Test pivot field tokenization
    print("\nPivot Field Tokenization:")
    pivot_field = "Sum of Total Revenue"
    pivot_tokens = PivotFieldTokenizer.tokenize_pivot_field(pivot_field)
    print(f"  Field: '{pivot_field}' -> {pivot_tokens}")