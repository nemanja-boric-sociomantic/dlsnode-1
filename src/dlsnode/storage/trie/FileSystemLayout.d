/********************************************************************************

    Implements DLS on-file-system-layout traversing.

    Copyright: Copyright (c) 2017 sociomantic labs GmbH. All rights reserved.

*******************************************************************************/

module dlsnode.storage.trie.FileSystemLayout;

import ocean.transition;
import ocean.io.model.IFile;
import ocean.core.Enforce;
import ocean.math.Math;
import ocean.math.Range;
import ocean.util.log.Log;
import Integer = ocean.text.convert.Integer_tango;

import dlsnode.storage.trie.util.PathUtils;
import dlsnode.storage.trie.fs.FileSystemCache;

/*******************************************************************************

    Calculates the first key that could be contained inside a bucket.

    Params:
        rel_path = relative path inside a channel directory.

    Returns:
        the first potential key this bucket could hold.

*******************************************************************************/

public hash_t get_start_file_hash (cstring rel_path)
{
    char[16] buf;
    cstring hash;

    auto i = 0;

    foreach (c; rel_path)
    {
        enforce (i < buf.length);

        if (c >= '0' && c <= '9' ||
            c >= 'a' && c <= 'f' ||
            c >= 'A' && c <= 'F')
        {
            buf[i++] = c;
        }
    }

    hash = buf[0..i];

    // adjust multiplier
    return Integer.parse(hash, 16) * pow(16UL, 16 - hash.length);
}

/*******************************************************************************

    Returns:
        true if the file name is a valid file name.

*******************************************************************************/

private bool valid_filename (cstring filepath)
{
    foreach (c; basename(filepath))
    {
        if (!((c >= '0' && c <= '9' ||
            c >= 'a' && c <= 'f' ||
            c >= 'A' && c <= 'F')))
        {
            return false;
        }
    }

    return true;
}
/// Iterates over the tree and prints files that are containing
/// data that we're looking forward to and call dg with the
/// files
void traverse_range_loop(FileSystem) (
    FileSystem tree,
    hash_t start_range, hash_t end_range,
    void delegate (Const!(DirectoryEntry)* file, cstring rel_path) dg)
{
    // It's not enough to know the file starting hash to see
    // if this file is good enough - it's important to know
    // the last hash, which is contained in the next entry
    // of the file system
    Const!(DirectoryEntry)* previous_file;

    tree.traverse((Const!(DirectoryEntry)* f, cstring rel_path)
    {
        // Skip directories: TODO make this configurable
        // at iteration time
        if (is_directory(*f))
            return;

        auto file_start_hash = get_start_file_hash(rel_path);

        // Save this file for the later iteration. If the
        // next bucket also covers values left of the
        // range, don't iterate over it
        if (file_start_hash <= start_range)
        {
            previous_file = f;
        }
        else
        {
            if (previous_file)
                dg(previous_file, rel_path);

            // check if we've at the end
            if (file_start_hash >= end_range)
            {
                // TODO: exit the iteration please
                // This requires this delegate to return integer,
                // breaking out from the previous as well
                previous_file = null;
            }
            else
            {
                previous_file = f;
            }
        }
    });
}

/*******************************************************************************

    Filter adaptor over the FileSystemCache's range filtering the buckets.

    Poor man's adaptation of Phobos' std.altorithm.iteration.filter;

*******************************************************************************/

struct LegacyFileSystemRange
{
    private FileSystemCache.FileSystemIteratorRange range;
    private hash_t start, end;
    private bool is_empty;
    private bool primed;

    public cstring front ()
    {
        this.prime();
        assert(!empty, "Attempting to fetch the front of an empty filter.");

        return range.relativePath;
    }

    public bool empty ()
    {
        this.prime();
        return range.empty || is_empty;
    }

    public void popFront ()
    {
        hash_t file_start_hash;

        auto required_range = Range!(hash_t).makeRange(this.start, this.end);

        do
        {
            if (!range.isValid()) return;
            range.popFront();

            if (range.empty) return;

            auto abs_path = range.front;
            auto rel_path = range.relativePath();

            if (!valid_filename(rel_path))
            {
                continue;
            }

            file_start_hash = get_start_file_hash(rel_path);

            if (file_start_hash >= this.end)
            {
                this.is_empty = true;
                return;
            }

            auto file_range = Range!(hash_t).makeRange(file_start_hash, file_start_hash + 4096);
            if (required_range.overlaps(file_range))
            {
                return;
            }
        }
        while (!range.empty);
    }

    public bool isValid ()
    {
        return this.range.isValid();
    }

    private void prime ()
    {
        if (this.primed) return;
        this.primed = true;

        hash_t file_start_hash;

        auto required_range = Range!(hash_t).makeRange(this.start, this.end);

        do
        {
            auto abs_path = range.front;
            auto rel_path = range.relativePath();

            file_start_hash = get_start_file_hash(rel_path);

            if (file_start_hash >= this.end)
            {
                this.is_empty = true;
                return;
            }

            auto file_range = Range!(hash_t).makeRange(file_start_hash, file_start_hash + 4096);
            if (required_range.overlaps(file_range))
            {
                return;
            }

            if (!range.isValid()) return;
            range.popFront();
        }
        while (!range.empty);
    }
}

public LegacyFileSystemRange traverseLegacy(FileSystemCache)(FileSystemCache fs,
        FileSystemBuffers* buffers,
        hash_t start, hash_t end)
{
    auto range = fs.traverse(buffers);
    auto filtered = LegacyFileSystemRange(range, start, end);
    return filtered;
}


version (UnitTest)
{
    import ocean.core.Test;
}

version(none) unittest
{
    auto tree = new FileSystemCache("/srv/dlsnode-02/data/test-nemanja");
    auto count_loop = 0;
    auto count_range = 0;

    // D1 requires explicitly instantiated template
    traverse_range_loop!(FileSystemCache)(tree, 0x51500000, 0x52760000,
        (Const!(DirectoryEntry)* f, cstring rel_path) {
            count_loop++;
        });

    for(auto filtered = traverseLegacy(tree, 0x51500000, 0x52760000);
            !filtered.empty; filtered.popFront())
    {
        count_range++;
    }

//    test!("==")(count_loop, count_range);
}
