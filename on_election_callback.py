"""
Python equivalent of OnElectionCallback.java
Interface/base class for election callbacks
"""

from abc import ABC, abstractmethod


class OnElectionCallback(ABC):

    @abstractmethod
    def on_elected_to_be_leader(self):
        pass

    @abstractmethod
    def on_worker(self):
        pass
