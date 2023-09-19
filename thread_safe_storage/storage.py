import hashlib
import threading
import re
import math
from typing import Union, Iterator
from contextlib import nullcontext

class Node: 
  def __init__(self, key, value) -> None:
    self._key: str = key
    self._value = value
    self._next: Union['Node', None] = None # used in case of collision
  
  def __repr__(self) -> str:
    return repr({'key': self.key, 'value': self.value})

  @property
  def key(self) -> str:
    return self._key
  
  @property
  def value(self):
    return self._value
  
  @value.setter
  def value(self, value) -> None:
    self._value = value
  
  @property
  def next(self) -> Union['Node', None]:
    return self._next
  
  @next.setter
  def next(self, next_node: 'Node') -> None:
    self._next = next_node

class DataStore():
  _is_multithreading = threading.active_count() > 1

  def __init__(self, capacity=10) -> None:
      self._cache = {}
      self._capacity = capacity
      self._lock = threading.Lock()
    
  @property
  def cache(self) -> dict:
    return self._cache
  
  @property
  def capacity(self) -> int:
    return self._capacity
  
  @property
  def size(self) -> int:
    return len(self._cache)
  
  @property
  def is_multithreading(self) -> bool:
    return self._is_multithreading

  def hash_func(self, key: str) -> str:
    # SHA-256 hash function implementation
    return hashlib.sha256(key.encode()).hexdigest()

  def contains(self, key: str) -> bool:
    return key in self.cache

  def resize(self, new_capacity: int) -> None:
    if new_capacity <= 0:
      raise RuntimeError('Capacity must be greater than 0')
    
    nodes = self.cache.values()
    self._capacity = new_capacity
    self._cache = {}

    for node in nodes:
      while node:
        self.insert(node.key, node.value)
        node = node.next
    
    return None

  def insert(self, key: str, value) -> Union['Node', None]:
    with self._lock if self._is_multithreading else nullcontext():
      hash_key = self.hash_func(key)
      node = Node(key, value)

      if self.size == self.capacity:
        self.resize(math.ceil(self.capacity * 1.5))
      
      if self.contains(hash_key):
        cur_node: 'Node' = self.cache[hash_key]
        print(cur_node.key)
        while cur_node:
          if cur_node.key == key: # TODO: How to handle duplicate keys? Ignore or Update value? Raise error?
            raise RuntimeError(f'Node with key {cur_node.key} already exists. Run update to change value')
            '''
              Below is how we could prompt the user to decide whetehr to overwrite the existing node with the same key
              
              acceptable_input = re.compile('[yn]')
              overwrite_input_response = input('A node with this key already exists. Would you like to overwrite the existing node? (Y/N): ').lower()
            
              while not acceptable_input.match(overwrite_input_response):
                print('Please respond with either Y for Yes or N for No.')
                overwrite_input_response = input('Would you like to overwrite the existing node? (Y/N): ').lower()
              
              if overwrite_input_response == 'y':
                cur_node.value = value
                return cur_node
              else:
                return None
            '''
          cur_node = cur_node.next

        node.next = self.cache[hash_key]

      self.cache[hash_key] = node
      return node

  def update(self, key: str, value) -> 'Node':
    with self._lock if self._is_multithreading else nullcontext():
      hash_key = self.hash_func(key)

      if self.cache[hash_key] is not None:
        cur_node = self.cache[hash_key]
        while cur_node:
          if cur_node.key == key:
            cur_node.value = value
            return cur_node
          cur_node = cur_node.next
      
      raise RuntimeError(f'No node with key {key} exists in cache')

  def delete(self, key: str) -> None:
    with self._lock if self._is_multithreading else nullcontext():
      hash_key = self.hash_func(key)

      if self.cache[hash_key] is not None:
        prev = None
        cur_node = self.cache[hash_key]
        while cur_node:
          if cur_node.key == key and prev:
            prev.next = cur_node.next
            return None
          elif cur_node.key == key and not prev: # node to delete was the first node in the bucket (linked list)
            self.cache[hash_key] = cur_node.next
            return None
          else:
            prev = cur_node
            cur_node = cur_node.next
      
      raise RuntimeError(f'No node with key {key} exists in cache')
    
  def search(self, key: str) -> Union['Node', None]:
    with self._lock if self._is_multithreading else nullcontext():
      hash_key = self.hash_func(key)

      if self.contains(hash_key):
        cur_node = self.cache[hash_key]
        while cur_node is not None and cur_node.key != key:
          cur_node = cur_node.next
        
        return cur_node if cur_node is not None else None
 
  def iterate(self) -> Iterator:
    with self._lock if self._is_multithreading else nullcontext():
      return iter(self.cache.items())
