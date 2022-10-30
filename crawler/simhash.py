import os
import shelve

from threading import Thread, RLock
from queue import Queue, Empty
from hashlib import blake2b
import numpy as np

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid


class SimHash:
    def __init__(self, config, restart):
        self.logger = get_logger("SIMHASH")
        self.config = config
        
        if not os.path.exists(self.config.simhash_file) and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find simhash file {self.config.simhash_file}, "
                f"create empty shelve.")
        elif os.path.exists(self.config.simhash_file) and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found simhash file {self.config.simhash_file}, deleting it.")
            os.remove(self.config.simhash_file)
        # Load existing simhash file, or create one if it does not exist.
        self.save = shelve.open(self.config.simhash_file)

    def store_simhash(self, url, word_freq):
        url = normalize(url)
        urlhash = get_urlhash(url)
        if urlhash not in self.save:
            self.save[url] = self._compute_simhash(word_freq)
            self.save.sync()

    def max_similarity(self, word_freq):
        V = self._compute_simhash(word_freq)
        max_url, max_sim = None, 0
        for url, vector in self.save.items():
            sim = (vector == V).mean()
            if sim > max_sim:
                max_sim = sim
                max_url = url
        return max_url, max_sim
    
    def is_near_duplicate(self, word_freq, threshold=0.9):
        V = self._compute_simhash(word_freq)
        for vector in self.save.values():
            if (vector == V).mean() > threshold:
                return True
        return False

    @staticmethod
    def _compute_simhash(word_freq, digest_size=32):
        """
        Parameters
        ----------
        word_freq: dict
            keys are tokens, values are the number of occurance of the token in a document.
        digest_size: int
            number of BYTES the hash function encodes the data into

        Returns
        -------
        np.ndarray
            1D binary array
        """
        simhash = np.zeros(digest_size * 8)
        for token, freq in word_freq.items():
            hash_bytes = blake2b(token.encode("utf-8"), digest_size=digest_size).digest()
            vector = []
            for byte_int in hash_bytes:
                for bit in bin(byte_int)[2:]:
                    bit = int(bit)
                    vector.append(1 if bit > 0 else -1)  # simhash treats 0 terms as negative sign for multiplication

            if len(vector) < digest_size * 8:
                vector.extend([-1 for _ in range(digest_size * 8 - len(vector))])
            # TODO: remove assert statement
            assert len(vector) == digest_size * 8, f"{digest_size * 8} != len(vector) {len(vector)}"
            simhash += freq * np.array(vector)
        
        simhash = (simhash > 0).astype(int)
        return simhash


if __name__ == "__main__":
    hash = blake2b("string")
    print(hash)