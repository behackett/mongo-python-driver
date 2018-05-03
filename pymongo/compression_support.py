# Copyright 2018 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import warnings

try:
    import snappy
    _HAVE_SNAPPY = True
except ImportError:
    # python-snappy isn't available.
    _HAVE_SNAPPY = False

try:
    import zlib
    _HAVE_ZLIB = True
except ImportError:
    # Python built without zlib support.
    _HAVE_ZLIB = False


_SUPPORTED_COMPRESSORS = set(["snappy", "zlib"])
_SENSITIVE_COMMANDS = set([
    "ismaster",
    "saslstart",
    "saslcontinue",
    "getnonce",
    "authenticate",
    "createuser",
    "updateuser",
    "copydbsaslstart",
    "copydbgetnonce",
    "copydb"])


def validate_compressors(dummy, value):
    compressors = value.split(",")
    for compressor in compressors[:]:
        if compressor not in _SUPPORTED_COMPRESSORS:
            compressors.remove(compressor)
            warnings.warn("Unknown compressor %s", (compressor,))
        elif compressor == "snappy" and not _HAVE_SNAPPY:
            raise ValueError(
                "You must install the python-snappy module for snappy support.")
        elif compressor == "zlib" and not _HAVE_ZLIB:
            raise ValueError("The zlib module is not available.")
    return compressors


def validate_zlib_compression_level(option, value):
    try:
        level = int(value)
    except:
        raise TypeError("%s must be an integer, not %r." % (option, value))
    if level < -1 or level > 9:
        raise ValueError(
            "%s must be between -1 and 9, not %d." % (option, value))


def decompress(data, compressor_id):
    if compressor_id == 1:
        return snappy.uncompress(data)
    elif compressor_id == 2:
        return zlib.decompress(data)
    else:
        raise ValueError("Unknown compressorId %d" % (compressor_id,))


class CompressionSettings(object):
    def __init__(self, compressors, zlib_compression_level):
        self.compressors = compressors
        self.zlib_compression_level = zlib_compression_level

    def get_compression_context(self, compressors):
        if compressors:
            chosen = compressors[0]
            if chosen == "snappy":
                return SnappyContext()
            elif chosen == "zlib":
                return ZlibContext(self.zlib_compression_level)


class SnappyContext(object):
    compressor_id = 1

    def compress(self, data):
        return snappy.compress(data)

class ZlibContext(object):
    compressor_id = 2

    def __init__(self, level):
        self._level = level

    def compress(self, data):
        return zlib.compress(data, self._level)
