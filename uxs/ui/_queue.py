from queue import Full
from collections import deque
import asyncio


class SymbolActionQueue:
    
    def __init__(self, maxsize=None):
        self.queue = deque(maxlen=maxsize)
        self.maxsize = maxsize
        self._wait_queue = asyncio.Queue()
    
    
    def put(self, future, action=None):
        if self.full():
            raise Full
        
        future.action = action
        self.queue.append(future)
        future.add_done_callback(lambda t: self.queue.remove(future))
        self._wait_queue.put_nowait(future)
    
    
    def get(self, action, default=None):
        try:
            return self[action]
        except KeyError:
            return default
    
    
    def full(self):
        return self.maxsize is not None and len(self.queue) >= self.maxsize
    
    
    def contains(self, action):
        actions = action
        
        if isinstance(action, str) or not hasattr(action, '__iter__'):
            actions = [action]
        
        return all(any(x.action==_action for x in self.queue)
                   for _action in actions)
    
    
    def __del__(self):
        self._wait_queue.put_nowait(None)
    
    def __getitem__(self, key):
        try:
            return next(x for x in self.queue if x.action==key)
        except StopIteration:
            raise KeyError(key)
    
    def __contains__(self, key):
        return self.contains(key)
    
    def __len__(self):
        return len(self.queue)
    
    def __bool__(self):
        return bool(self.__len__())
