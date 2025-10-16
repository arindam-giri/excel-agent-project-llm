"""
Unified cache management for Excel Agent

Implements multi-tier caching strategy:
- Tier 1: In-memory LRU cache (fastest, limited size)
- Tier 2: Disk cache with persistence (larger, survives restarts)
- Tier 3: File-based cache for large objects (CSV files, etc.)

Cache invalidation based on file modification time and TTL
"""

import hashlib
import json
import pickle
import time
from pathlib import Path
from typing import Any, Optional, Dict, Callable, Union
from functools import wraps
from cachetools import LRUCache
import diskcache
from filelock import FileLock
from config.settings import settings
from src.utils.logger import logger, log_cache_operation


class CacheKey:
    """Helper class to generate consistent cache keys"""
    
    @staticmethod
    def for_file(file_path: Union[str, Path]) -> str:
        """
        Generate cache key for a file based on path and modification time
        
        Args:
            file_path: Path to file
            
        Returns:
            Cache key string
        """
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        mtime = file_path.stat().st_mtime
        # Use file path and mtime for key
        key_data = f"{file_path.absolute()}:{mtime}"
        return hashlib.md5(key_data.encode()).hexdigest()
    
    @staticmethod
    def for_query(query: str, context: Optional[Dict] = None) -> str:
        """
        Generate cache key for a search query
        
        Args:
            query: Search query string
            context: Additional context (filters, etc.)
            
        Returns:
            Cache key string
        """
        key_data = query
        if context:
            key_data += json.dumps(context, sort_keys=True)
        
        return hashlib.md5(key_data.encode()).hexdigest()
    
    @staticmethod
    def for_operation(operation: str, *args, **kwargs) -> str:
        """
        Generate cache key for an operation with arguments
        
        Args:
            operation: Operation name
            *args: Positional arguments
            **kwargs: Keyword arguments
            
        Returns:
            Cache key string
        """
        key_parts = [operation]
        
        # Add args
        for arg in args:
            if isinstance(arg, (str, int, float, bool)):
                key_parts.append(str(arg))
            elif isinstance(arg, Path):
                key_parts.append(str(arg.absolute()))
            else:
                key_parts.append(str(hash(str(arg))))
        
        # Add kwargs
        if kwargs:
            key_parts.append(json.dumps(kwargs, sort_keys=True))
        
        key_data = ":".join(key_parts)
        return hashlib.md5(key_data.encode()).hexdigest()


class CacheManager:
    """
    Unified cache manager with multi-tier strategy
    
    Manages three cache tiers:
    1. Memory cache (LRU) - fastest, small
    2. Disk cache (diskcache) - medium speed, larger
    3. File cache - slowest, largest (for CSV files)
    """
    
    def __init__(self):
        self.enabled = settings.enable_cache
        
        # Tier 1: In-memory LRU cache
        self.memory_cache = LRUCache(maxsize=settings.memory_cache_size)
        
        # Tier 2: Disk cache
        if self.enabled:
            self.disk_cache = diskcache.Cache(
                directory=str(settings.cache_dir / "disk_cache"),
                size_limit=settings.cache_size_limit_bytes,
                timeout=1.0
            )
        else:
            self.disk_cache = None
        
        # Tier 3: File cache directory
        self.file_cache_dir = settings.cache_dir / "file_cache"
        self.file_cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Cache statistics
        self.stats = {
            'memory_hits': 0,
            'memory_misses': 0,
            'disk_hits': 0,
            'disk_misses': 0,
            'file_hits': 0,
            'file_misses': 0,
        }
        
        logger.info(f"CacheManager initialized - Enabled: {self.enabled}")
    
    def get(self, key: str, tier: str = "auto") -> Optional[Any]:
        """
        Get value from cache
        
        Args:
            key: Cache key
            tier: Cache tier to check ("memory", "disk", "auto")
            
        Returns:
            Cached value or None if not found
        """
        if not self.enabled:
            return None
        
        # Check memory cache first (if auto or memory)
        if tier in ("auto", "memory"):
            if key in self.memory_cache:
                self.stats['memory_hits'] += 1
                log_cache_operation("get", key, hit=True)
                return self.memory_cache[key]
            else:
                self.stats['memory_misses'] += 1
        
        # Check disk cache (if auto or disk)
        if tier in ("auto", "disk") and self.disk_cache is not None:
            try:
                value = self.disk_cache.get(key, default=None)
                if value is not None:
                    self.stats['disk_hits'] += 1
                    # Promote to memory cache
                    self.memory_cache[key] = value
                    log_cache_operation("get", key, hit=True)
                    return value
                else:
                    self.stats['disk_misses'] += 1
            except Exception as e:
                logger.warning(f"Disk cache read error: {e}")
                self.stats['disk_misses'] += 1
        
        log_cache_operation("get", key, hit=False)
        return None
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None, 
            tier: str = "auto") -> bool:
        """
        Set value in cache
        
        Args:
            key: Cache key
            value: Value to cache
            ttl: Time to live in seconds (None = use default)
            tier: Cache tier ("memory", "disk", "auto")
            
        Returns:
            True if successful
        """
        if not self.enabled:
            return False
        
        ttl = ttl or settings.cache_ttl_seconds
        
        try:
            # Always store in memory cache (for speed)
            if tier in ("auto", "memory"):
                self.memory_cache[key] = value
            
            # Store in disk cache for persistence
            if tier in ("auto", "disk") and self.disk_cache is not None:
                # Store with TTL
                expire_time = time.time() + ttl
                self.disk_cache.set(key, value, expire=expire_time)
            
            # Calculate size for logging
            try:
                size = len(pickle.dumps(value))
            except:
                size = None
            
            log_cache_operation("set", key, size_bytes=size)
            return True
            
        except Exception as e:
            logger.error(f"Cache set error for key {key}: {e}")
            return False
    
    def delete(self, key: str) -> bool:
        """
        Delete key from all cache tiers
        
        Args:
            key: Cache key
            
        Returns:
            True if key was found and deleted
        """
        if not self.enabled:
            return False
        
        found = False
        
        # Delete from memory
        if key in self.memory_cache:
            del self.memory_cache[key]
            found = True
        
        # Delete from disk
        if self.disk_cache is not None:
            if self.disk_cache.delete(key):
                found = True
        
        if found:
            log_cache_operation("delete", key)
        
        return found
    
    def clear(self, tier: str = "all") -> None:
        """
        Clear cache
        
        Args:
            tier: Which tier to clear ("memory", "disk", "file", "all")
        """
        if not self.enabled:
            return
        
        if tier in ("memory", "all"):
            self.memory_cache.clear()
            logger.info("Memory cache cleared")
        
        if tier in ("disk", "all") and self.disk_cache is not None:
            self.disk_cache.clear()
            logger.info("Disk cache cleared")
        
        if tier in ("file", "all"):
            self._clear_file_cache()
            logger.info("File cache cleared")
    
    def _clear_file_cache(self) -> None:
        """Clear file-based cache"""
        import shutil
        if self.file_cache_dir.exists():
            shutil.rmtree(self.file_cache_dir)
            self.file_cache_dir.mkdir(parents=True, exist_ok=True)
    
    def get_file_path(self, key: str, suffix: str = ".cache") -> Path:
        """
        Get file path for file-based cache entry
        
        Args:
            key: Cache key
            suffix: File suffix
            
        Returns:
            Path to cache file
        """
        # Create subdirectories based on first 2 chars of key (for better file system performance)
        subdir = self.file_cache_dir / key[:2]
        subdir.mkdir(parents=True, exist_ok=True)
        
        return subdir / f"{key}{suffix}"
    
    def cache_file(self, key: str, source_path: Union[str, Path], 
                   ttl: Optional[int] = None) -> Path:
        """
        Cache a file (copy to cache directory)
        
        Args:
            key: Cache key
            source_path: Path to source file
            ttl: Time to live in seconds
            
        Returns:
            Path to cached file
        """
        if not self.enabled:
            return Path(source_path)
        
        source_path = Path(source_path)
        suffix = source_path.suffix
        cached_path = self.get_file_path(key, suffix)
        
        # Copy file if not already cached
        if not cached_path.exists():
            import shutil
            shutil.copy2(source_path, cached_path)
            logger.debug(f"File cached: {source_path.name} -> {cached_path.name}")
        
        # Store metadata in disk cache
        metadata = {
            'original_path': str(source_path.absolute()),
            'cached_path': str(cached_path),
            'created_at': time.time(),
            'ttl': ttl or settings.cache_ttl_seconds
        }
        self.set(f"{key}_metadata", metadata, ttl=ttl)
        
        return cached_path
    
    def get_cached_file(self, key: str) -> Optional[Path]:
        """
        Get cached file path if it exists and is valid
        
        Args:
            key: Cache key
            
        Returns:
            Path to cached file or None
        """
        if not self.enabled:
            return None
        
        metadata = self.get(f"{key}_metadata", tier="disk")
        if metadata is None:
            return None
        
        cached_path = Path(metadata['cached_path'])
        
        # Check if file still exists
        if not cached_path.exists():
            self.delete(f"{key}_metadata")
            return None
        
        # Check TTL
        created_at = metadata['created_at']
        ttl = metadata['ttl']
        if time.time() - created_at > ttl:
            # Expired - delete
            cached_path.unlink(missing_ok=True)
            self.delete(f"{key}_metadata")
            return None
        
        return cached_path
    
    def invalidate_file_cache(self, file_path: Union[str, Path]) -> None:
        """
        Invalidate all cache entries related to a file
        
        Args:
            file_path: Path to file
        """
        file_path = Path(file_path)
        
        # Generate all possible keys for this file
        if file_path.exists():
            try:
                file_key = CacheKey.for_file(file_path)
                # Delete all keys starting with this file's key
                if self.disk_cache is not None:
                    # Get all keys and filter
                    for key in list(self.disk_cache.iterkeys()):
                        if key.startswith(file_key):
                            self.delete(key)
                logger.info(f"Invalidated cache for file: {file_path.name}")
            except Exception as e:
                logger.warning(f"Error invalidating file cache: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics
        
        Returns:
            Dict with cache stats
        """
        stats = self.stats.copy()
        
        # Calculate hit rates
        total_memory = stats['memory_hits'] + stats['memory_misses']
        total_disk = stats['disk_hits'] + stats['disk_misses']
        
        if total_memory > 0:
            stats['memory_hit_rate'] = stats['memory_hits'] / total_memory
        else:
            stats['memory_hit_rate'] = 0.0
        
        if total_disk > 0:
            stats['disk_hit_rate'] = stats['disk_hits'] / total_disk
        else:
            stats['disk_hit_rate'] = 0.0
        
        # Get cache sizes
        stats['memory_cache_size'] = len(self.memory_cache)
        
        if self.disk_cache is not None:
            stats['disk_cache_size'] = len(self.disk_cache)
            stats['disk_cache_bytes'] = self.disk_cache.volume()
        else:
            stats['disk_cache_size'] = 0
            stats['disk_cache_bytes'] = 0
        
        return stats
    
    def __del__(self):
        """Cleanup on deletion"""
        if self.disk_cache is not None:
            self.disk_cache.close()


# Global cache manager instance
_cache_manager = None

def get_cache_manager() -> CacheManager:
    """Get global cache manager instance"""
    global _cache_manager
    if _cache_manager is None:
        _cache_manager = CacheManager()
    return _cache_manager


# Decorator for caching function results
def cached(key_prefix: str = "", ttl: Optional[int] = None, 
           tier: str = "auto", use_args: bool = True):
    """
    Decorator to cache function results
    
    Args:
        key_prefix: Prefix for cache key
        ttl: Time to live in seconds
        tier: Cache tier to use
        use_args: If True, include function arguments in cache key
        
    Example:
        @cached(key_prefix="search", ttl=300)
        def search_excel(query, file_path):
            # expensive operation
            return results
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            cache = get_cache_manager()
            
            if not cache.enabled:
                return func(*args, **kwargs)
            
            # Generate cache key
            if use_args:
                cache_key = CacheKey.for_operation(
                    f"{key_prefix}:{func.__name__}",
                    *args,
                    **kwargs
                )
            else:
                cache_key = f"{key_prefix}:{func.__name__}"
            
            # Try to get from cache
            result = cache.get(cache_key, tier=tier)
            if result is not None:
                logger.debug(f"Cache hit for {func.__name__}")
                return result
            
            # Execute function
            logger.debug(f"Cache miss for {func.__name__}, executing...")
            result = func(*args, **kwargs)
            
            # Store in cache
            cache.set(cache_key, result, ttl=ttl, tier=tier)
            
            return result
        
        return wrapper
    return decorator


# Context manager for temporary cache disabling
class DisableCache:
    """Context manager to temporarily disable caching"""
    
    def __init__(self):
        self.cache = get_cache_manager()
        self.original_state = None
    
    def __enter__(self):
        self.original_state = self.cache.enabled
        self.cache.enabled = False
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cache.enabled = self.original_state
        return False


# Specialized cache for Excel metadata
class ExcelMetadataCache:
    """Specialized cache for Excel file metadata"""
    
    def __init__(self):
        self.cache = get_cache_manager()
    
    def get_metadata(self, file_path: Union[str, Path]) -> Optional[Dict]:
        """
        Get cached metadata for Excel file
        
        Args:
            file_path: Path to Excel file
            
        Returns:
            Cached metadata or None
        """
        if not settings.cache_excel_metadata:
            return None
        
        try:
            key = f"excel_metadata:{CacheKey.for_file(file_path)}"
            return self.cache.get(key, tier="disk")
        except Exception as e:
            logger.warning(f"Error getting cached metadata: {e}")
            return None
    
    def set_metadata(self, file_path: Union[str, Path], 
                     metadata: Dict, ttl: Optional[int] = None) -> bool:
        """
        Cache Excel file metadata
        
        Args:
            file_path: Path to Excel file
            metadata: Metadata dict
            ttl: Time to live
            
        Returns:
            True if successful
        """
        if not settings.cache_excel_metadata:
            return False
        
        try:
            key = f"excel_metadata:{CacheKey.for_file(file_path)}"
            return self.cache.set(key, metadata, ttl=ttl, tier="disk")
        except Exception as e:
            logger.error(f"Error caching metadata: {e}")
            return False
    
    def invalidate(self, file_path: Union[str, Path]) -> None:
        """Invalidate metadata cache for a file"""
        try:
            key = f"excel_metadata:{CacheKey.for_file(file_path)}"
            self.cache.delete(key)
        except Exception:
            pass


# Export main components
__all__ = [
    'CacheManager',
    'CacheKey',
    'get_cache_manager',
    'cached',
    'DisableCache',
    'ExcelMetadataCache',
]


if __name__ == "__main__":
    # Test cache manager
    print("=== Cache Manager Tests ===\n")
    
    cache = get_cache_manager()
    
    # Test basic operations
    print("1. Testing basic cache operations:")
    cache.set("test_key", {"data": "test_value"}, ttl=60)
    result = cache.get("test_key")
    print(f"   Set and get: {result}")
    
    # Test cache key generation
    print("\n2. Testing cache key generation:")
    file_key = CacheKey.for_operation("search", "query", file_path="/tmp/test.xlsx")
    print(f"   Operation key: {file_key}")
    
    # Test decorator
    print("\n3. Testing cache decorator:")
    
    @cached(key_prefix="test", ttl=60)
    def expensive_function(x, y):
        print(f"   Computing {x} + {y}...")
        return x + y
    
    result1 = expensive_function(5, 3)
    print(f"   First call: {result1}")
    
    result2 = expensive_function(5, 3)
    print(f"   Second call (cached): {result2}")
    
    # Test cache stats
    print("\n4. Cache statistics:")
    stats = cache.get_stats()
    for key, value in stats.items():
        print(f"   {key}: {value}")
    
    # Test context manager
    print("\n5. Testing cache disable context:")
    with DisableCache():
        result3 = expensive_function(5, 3)
        print(f"   Call with cache disabled: {result3}")
    
    # Clear cache
    print("\n6. Clearing cache...")
    cache.clear()
    print("   Cache cleared")
    
    print("\n=== Tests Complete ===")