import gzip
import tempfile


def compress(string, level=9, bufsize=-1):
    compressedtext = None

    with tempfile.NamedTemporaryFile("wr", bufsize, "gziplib") as ntf:

        with gzip.GzipFile(ntf.name, "w", level) as gzf:
            gzf.write(string)

        with open(ntf.name, "r") as gzf:
            compressedtext = gzf.read()

    return compressedtext


def decompress(string, bufsize=-1):
    plaintext = None

    with tempfile.NamedTemporaryFile("wr", bufsize, "gziplib") as ntf:
        with open(ntf.name, "w") as gzf:
            gzf.write(string)

        with gzip.GzipFile(ntf.name, "r") as gzf:
            plaintext = gzf.read()

    return plaintext
