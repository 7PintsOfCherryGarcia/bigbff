/*
The MIT License

Copyright (c) 2008-     7pints <jregalado@bicu.dev>

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>

#include "binaryfusefilter.h"
#include "kthread.h"

#define SIZE  10000000000UL //10 billion keys
#define MSIZE 250000000U    //Max subfilter size

typedef binary_fuse8_t bf8_t;
typedef binary_fuse16_t bf16_t;
typedef binary_fuse32_t bf32_t;

typedef struct bffc32_t {
    uint32_t n;
    bf32_t *bffc;
    uint64_t *keys;
    uint64_t size;
} bffc32_t;

typedef struct bffc16_t {
    uint32_t n;
    bf16_t *bffc;
    uint64_t *keys;
    uint64_t size;
} bffc16_t;

typedef struct bffc8_t {
    uint32_t n;
    bf8_t *bffc;
    uint64_t *keys;
    uint64_t size;
} bffc8_t;

bffc32_t *bffc32_init(uint32_t n)
{
    bffc32_t *filterc = calloc(1, sizeof(bffc32_t));
    if (!filterc) return NULL;
    filterc->n = n;
    filterc->bffc = calloc(n, sizeof(bf32_t));
    if (!filterc->bffc) {
        free(filterc);
        return NULL;
    }
    return filterc;
}

void bffc32_destroy(bffc32_t *filterc)
{
    for (uint32_t i = 0; i < filterc->n; i++)
        binary_fuse32_free(&filterc->bffc[i]);
    free(filterc->bffc);
    free(filterc);
}

int8_t bffc32_allocate(bffc32_t *filterc, uint64_t size)
{
    int8_t ret = 1;
    uint32_t n = filterc->n;
    uint32_t subfiltersize = size / n;
    uint32_t i;
    for (i = 0; i < n - 1; i++) {
        if ( !binary_fuse32_allocate(subfiltersize, &filterc->bffc[i]) )
            goto exit;
    }
    if ( !binary_fuse32_allocate(subfiltersize + (size % n), &filterc->bffc[i]) )
            goto exit;
    ret = 0;
    exit:
        if (ret) {
            //Free succesfully allocated filters
            for (uint32_t j = 0; j < i - 1; j++)
                binary_fuse32_free(&filterc->bffc[i]);
        }
        return ret;
}

void populate_for(void *data, long i, int tid)
{
    bffc32_t *filterc = (bffc32_t *)data;
    uint64_t size = filterc->size;
    uint32_t n = filterc->n;
    uint32_t subfiltersize = size / n + (i==(n-1) ? (size % n) : 0);
    uint32_t idx = i * subfiltersize;
    uint64_t *keys = filterc->keys + idx;
    //fprintf(stderr, "\t%ld - %u - %u\n", i, idx, subfiltersize);
    binary_fuse32_populate(keys, subfiltersize, &filterc->bffc[i]);
}

int8_t bffc32_populate(bffc32_t *filterc, uint64_t *keys, uint64_t size, uint8_t t)
{
    //TODO figure out what to do when filter populate fails
    uint32_t n = filterc->n;
    filterc->keys = keys;
    filterc->size = size;
    //Parallel for loop (see kthread.c)
    kt_for(t, populate_for, (void *)filterc, n);
    return 1;
}

uint64_t bffc32_nquery(bffc32_t *filterc, uint64_t *keys, uint64_t size)
{
    uint32_t n = filterc->n;
    bf32_t *filters = filterc->bffc;
    uint64_t found = 0;
    for (uint64_t i = 0; i < size; i++) {
        uint64_t key = keys[i];
        for (uint32_t j = 0; j < n; j++) {
            if ( binary_fuse32_contain( key, &filters[j]) ) {
                found++;
                break;
            }
        }
    }
    return found;
}

int main(int argc, char *argv[])
{
    if (argc < 3) return -1;
    uint64_t *keys = NULL;
    uint8_t nfilters = atoi(argv[1]);
    uint8_t nthreads = atoi(argv[2]);
    if ( (nfilters == 0) || (nthreads == 0) ) return -1;
    uint32_t subfiltsize = SIZE / nfilters;
    if (subfiltsize > MSIZE) return -1;
    fprintf(stderr, "Dividing input data into %u chunks of size: %u\n", nfilters, subfiltsize);
    bffc32_t *filterc = bffc32_init(nfilters);
    if (!filterc) goto exit;     
    
    fprintf(stderr, "Allocating filters\n");
    bffc32_allocate(filterc, SIZE);
   
    srand(time(NULL));
    uint64_t rng_counter = (uint64_t)rand();
    
    keys = calloc(SIZE, sizeof(uint64_t));
    if (!keys) goto exit;
    //Add random keys
    for (uint64_t i = 0; i < SIZE; i++)
        keys[i] = binary_fuse_rng_splitmix64(&rng_counter);
    
    fprintf(stderr, "Populating filters\n");
    bffc32_populate(filterc, keys, SIZE, nthreads);

    //Change half the keys
    fprintf(stderr, "Creating queries\n");
    for (uint64_t i = 0; i < SIZE; i++)
        keys[i] = (i & 1U) ? keys[i] : binary_fuse_rng_splitmix64(&rng_counter);
    fprintf(stderr, "Querying\n");
    uint64_t found = bffc32_nquery(filterc, keys, SIZE);
    fprintf(stderr, "Found %lu queries\n", found);
    exit:
        if (keys) free(keys);
        return 0;
}
