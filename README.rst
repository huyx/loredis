loredis
=======

Pure python reader for redis::

    import loredis

    reader = loredis.Reader()
    reader.feed(b'...')
    reader.gets()

Also privide a ServerReader, which accept inline command.

    import loredis

    reader = loredis.ServerReader()
    ...

Also privide some function to build command:

- INT
- SIMPLE_STRING
- ERROR
- BULK_STRING
- ARRAY
- build_command(BULK_STRING_ARRAY)
