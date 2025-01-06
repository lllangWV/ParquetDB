import os
import shutil
import uuid
from filelock import FileLock  # pip install filelock

class TransactionManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self._lock_file = os.path.join(db_path, ".db_lock")
        self._temp_dir = None
        self._active_tx = False
        self._lock = FileLock(self._lock_file)

    def begin_transaction(self):
        # Acquire exclusive lock so no other writes happen
        self._lock.acquire()
        self._active_tx = True
        
        # Create a temp directory for staging any new or updated Parquet files
        self._temp_dir = os.path.join(self.db_path, f"temp_tx_{uuid.uuid4()}")
        os.makedirs(self._temp_dir, exist_ok=True)

    def commit(self):
        if not self._active_tx:
            raise RuntimeError("No active transaction to commit.")

        # Atomically move from temp directory to the main DB folder
        self._apply_changes()

        # Clean up
        self._cleanup()

    def rollback(self):
        if not self._active_tx:
            raise RuntimeError("No active transaction to rollback.")

        # Just discard the temp changes
        self._cleanup()

    def _apply_changes(self):
        """
        Move or rename new Parquet files from the temp directory into the main DB folder 
        in an atomic way. For instance, you might rename or replace the original dataset
        with the newly generated dataset.
        """
        for fname in os.listdir(self._temp_dir):
            src = os.path.join(self._temp_dir, fname)
            dst = os.path.join(self.db_path, fname)
            
            # If you want versioning, you could move the old files to a “backup” or 
            # “versions” folder. For simplicity, we replace them:
            if os.path.exists(dst):
                os.remove(dst)
            
            # Atomic operation: rename in the same filesystem
            os.rename(src, dst)

    def _cleanup(self):
        # Remove the temp directory and release the lock
        if os.path.exists(self._temp_dir):
            shutil.rmtree(self._temp_dir)
        self._active_tx = False
        self._lock.release()
