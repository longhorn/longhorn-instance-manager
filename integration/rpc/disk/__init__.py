import os
import sys


# include current directory to fix relative import in generated grpc files
sys.path.append(
    os.path.abspath(
        os.path.join(os.path.split(__file__)[0], ".")
    )
)
