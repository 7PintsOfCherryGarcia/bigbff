# bigbff
An implementation of building [binary fuse filters](https://github.com/FastFilter/xor_singleheader) on large datasets

This is just a small example on how to build binary fuse filters on large datasets (> $2^{32}$ entries)

It relies on spliting the input keys on equally sized "chunks" and building a binary fuse filter with each chunk.
Filter building happens in-memory with no writes or reads to disk. Thus, it requires a very large amount of system ram ~400GB.

This example constructs a "chunked" filter from 10 billion keys. The user provides how many chunks the dataset must be divided by and the number of threads to be used. Chunks have a maximum size of 250 million keys.

The threading model is provided by Heng Li's kthread library, see [klib](https://github.com/attractivechaos/klib)

To compile:

```
gcc -pedantic -Wall -Wextra -O3 -c -o kthread.o kthread.c
gcc -pedantic -Wall -Wextra -std=c11 -O3 -o bigbff bigbff.c kthread.o -lm -lpthread
```

Code in this repo uses 32 bit fingerprints you can use 16 bit and 8 bit fingerprints by changing to the types:

```
bffc16_t
//or
bffc8_t
```

Although I haven't implemented functions to handle these types.
