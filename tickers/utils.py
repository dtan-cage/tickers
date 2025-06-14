import os
import time


class DirManager(object):
    """Context manager for controlled entry and exit of directories.

    Args:
        dst: Path of the dir to manage.
        verbose: If True, print messages of the current actions.

    """

    def __init__(self, dst: str, verbose: bool = False) -> None:
        self.topdir = os.getcwd()
        self.dst = os.path.join(self.topdir, dst)
        self.verbose = verbose

    def __enter__(self) -> None:
        if not os.access(self.dst, os.F_OK):
            if self.verbose:
                print(time.strftime("%c"), end="")
                print(f"Creating directory {self.dst}")
            try:
                os.makedirs(self.dst)
            except OSError:
                time.sleep(5)
        if self.verbose:
            print(time.strftime("%c"), end="")
            print(f"Entering directory {self.dst}")
        os.chdir(self.dst)
        return

    def __exit__(self, typ, value, traceback) -> None:
        if self.verbose:
            print(time.strftime("%c"), end="")
            print(f"Entering directory {self.topdir}")
        os.chdir(self.topdir)
        return
