/********************************************************************************

    FileSystem cache. This structure is representing the file system cache
    with the ability for traversing the file system. Initially, the file system
    is queried and the initial build is made, then all the updates should
    be performed by the user.

    Copyright: Copyright (c) 2017 sociomantic labs GmbH. All rights reserved.

*******************************************************************************/

module dlsnode.storage.trie.fs.FileSystemCache;

import ocean.core.array.Mutation;
import ocean.core.array.Search;
import ocean.core.Enforce;
import ocean.core.SmartUnion;
import ocean.core.Verify;
import ocean.io.FilePath_tango;
import ocean.io.model.IFile;
import ocean.transition;
import ocean.text.convert.Formatter;
import ocean.util.container.btree.BTreeMap;
import ocean.util.container.btree.BTreeMapRange;

import dlsnode.storage.trie.util.PathUtils;

version(UnitTest)
{
    import ocean.core.Test;
}

/*******************************************************************************

    Structure encapsulating the buffers needed for the functioning ranges.

*******************************************************************************/

struct FileSystemBuffers
{
    mstring path_buffer;
    ubyte[] stack_buffer;
    ubyte[] tree_buffer;
    mstring filename_buffer;


    public void reset ()
    {
        foreach (i, field; this.tupleof)
        {
            this.tupleof[i].length = 0;
            enableStomping(this.tupleof[i]);
        }
    }
}

/*******************************************************************************

    DirectoryEntry name.

*******************************************************************************/

struct DirectoryName
{
    private size_t name_length;
    private char[50] name_buf;

    public void name (in cstring name)
    {
        enforce (name.length <= name_buf.length);

        this.name_length = name.length;
        this.name_buf[0..this.name_length] = name[];
    }

    public cstring name() /* d1to2fix_inject: const */
    {
        return this.name_buf[0..this.name_length];
    }

    mixin (genOpCmp("
            {
                if (this.name >= rhs.name)
                    return this.name > rhs.name;
                return -1;
            }
            "));
}

/*******************************************************************************

    Workaround for not being able to put `BTree!(DirectoryEntry, Degree, Allocator)*`
    directly into struct DirectoryEntry, because of the forward reference errors
    Ideally, these all should be contained inside `struct File`'s definition.

*******************************************************************************/

public struct DirectoryListing
{
    /// Degree of the tree. Should be choosen wisely, as it
    /// is the compromise of the wasted space (if not used) and speed
    const ChildrenTreeDegree = 51;

    /// Directory items
    BTreeMap!(DirectoryName, DirectoryEntry, ChildrenTreeDegree) entries;

    static DirectoryListing* createListing ()
    {
        auto listing = cast(DirectoryListing*)((new ubyte[DirectoryListing.sizeof]).ptr);
        *listing = DirectoryListing(makeBTreeMap!(DirectoryName,
                    DirectoryEntry, ChildrenTreeDegree));
        return listing;
    }
}

/*******************************************************************************

    Directory entry structure - represents a directory or a file

*******************************************************************************/

public struct DirectoryEntry
{
    /***************************************************************************

        Pointer to the parent directory

    ***************************************************************************/

    private DirectoryEntry* parent;

    /***************************************************************************

        Directory subentries.

    ***************************************************************************/

    private DirectoryListing* children;

    /***************************************************************************

        Adds a subdirectory to this directory.

        Params:
            name = name of the new subdirectory.

    ***************************************************************************/

    public DirectoryEntry* addSubdirectory (cstring name)
    {
        assert (is_directory(*this));

        DirectoryEntry dir;
        dir.parent = this;
        dir.type = DirectoryEntry.Type.Directory;
        dir.children = DirectoryListing.createListing();
        dir.children.entries = makeBTreeMap!(DirectoryName, DirectoryEntry,
                DirectoryListing.ChildrenTreeDegree);
        dir.name = name;

        DirectoryName directory_name;
        directory_name.name = name;

        verify(this.children.entries.insert(directory_name, dir));
        return this.children.entries.get(directory_name);
    }

    /***************************************************************************

        Adds a file to this directory.

        Params:
            name = name of the new file

        Returns:
            pointer to the newly created file

    ***************************************************************************/

    public void addFile (cstring name)
    {
        assert (is_directory(*this));

        DirectoryEntry file;
        file.parent = this;
        file.type = DirectoryEntry.Type.File;
        file.name = name;

        DirectoryName filename;
        filename.name = name;

        verify(this.children.entries.insert(filename, file));
    }

    /***************************************************************************

        Removes a file from the directory

        Params:
            name = name of the file to delete

    ***************************************************************************/

    public void deleteFile (cstring name)
    {
        assert (is_directory(*this));

        DirectoryName dir_name;
        dir_name.name = name;

        // Ignoring return value since further removals are noops
        this.children.entries.remove(dir_name);
    }

    /***************************************************************************

        Type of this directory entry.

    ***************************************************************************/

    private enum Type
    {
        File,
        Directory
    }

    /// ditto
    private Type type;

    private size_t name_length;
    private char[50] name_buf;

    public void name (in cstring name)
    {
        enforce (name.length <= name_buf.length);

        this.name_length = name.length;
        this.name_buf[0..this.name_length] = name[];
    }

    public cstring name() /* d1to2fix_inject: const */
    {
        return this.name_buf[0..this.name_length];
    }
}


/*******************************************************************************

    Checks if the directory entry is a file or a folder.

    Params:
        direntry = directory entry to check

    Returns:
        true if the directory entry is a directory, false if it's a file

*******************************************************************************/

public bool is_directory (ref Const!(DirectoryEntry) direntry)
{
    return direntry.type == DirectoryEntry.Type.Directory;
}

/*******************************************************************************

    Directory iterator based on the FilePath scope class.
    Used for iterating the real file system

*******************************************************************************/

public struct FilePathIterator
{
    /***************************************************************************

        Path of the directory to iterate over.

    ***************************************************************************/

    private cstring path;

    int opApply (int delegate(ref FileInfo) dg)
    {
        scope file_path = new FilePath(path);
        return file_path.opApply(dg);
    }
}

/*******************************************************************************

    Default implementation of the FileSystemCache.

*******************************************************************************/

public alias FileSystemCacheImpl!(FilePathIterator) FileSystemCache;

/*******************************************************************************

    FileSystemCache implementation.

    Params:
        DirectoryIterator = iterator over the directory. By default is a iterator
        that iterates over the file system, but could be anything that implements
        a opApply.

*******************************************************************************/

public class FileSystemCacheImpl(alias DirectoryIterator = FilePathIterator)
{
    /***************************************************************************

        Path of the top-level directory.

    ***************************************************************************/

    private cstring root_path;

    /***************************************************************************

        Top-level directory entry.

    ***************************************************************************/

    private DirectoryEntry root;

    /***************************************************************************

        Path buffer for used for building the path over.

    ***************************************************************************/

    private PathBuffer path_buffer;

    /***************************************************************************

        Constructor.

        Scans the directory and builds the initial cache.

    ***************************************************************************/

    public this (cstring root_path)
    {
        enforce(root_path.length);

        // Normalize the trailing /
        if (root_path[$-1] == '/')
            root_path = root_path[0..$-1];
        this.root_path = root_path;

        root.type = DirectoryEntry.Type.Directory;
        // no name for the root directory
        root.children = DirectoryListing.createListing();
        root.children.entries = makeBTreeMap!(DirectoryName, DirectoryEntry,
                DirectoryListing.ChildrenTreeDegree);

        this.path_buffer.init();

        build(this.path_buffer, this.root_path, root);
    }

    /***************************************************************************

        Traverses the file system and fills the directory
        cache
        Params:
            current path buffer
            path to iterate over
            root_dir = root directory of the paths

    ***************************************************************************/

    private void build (ref PathBuffer current_path,
        cstring path, ref DirectoryEntry root_dir)
    {
        // Build the current level's path
        current_path.enterPath(path);
        scope (exit) current_path.exitPath(path);

        auto dir_path = DirectoryIterator(current_path.data());

        foreach (item; dir_path)
        {
            if (item.folder)
            {
                // allocate new child folder
                auto dir = root_dir.addSubdirectory(item.name);
                build(current_path, item.name, *dir);
            }
            else
            {
                root_dir.addFile(item.name);
            }
        }
    }

    /**************************************************************************

        Range struct for iterating over the entries in the file system.

    ***************************************************************************/

    static struct FileSystemIteratorRange
    {
        private PathBuffer path_buffer;

        /***********************************************************************

            Top-level directory.

        ***********************************************************************/

        private DirectoryEntry* root;

        /***********************************************************************

            Path of the top level directory.

        ***********************************************************************/

        private cstring root_path;

        /***********************************************************************

            Type of the range used for iterating over the subdirectories.

        ***********************************************************************/

        private alias BTreeMapRange!(typeof(DirectoryListing.entries)) SubdirectoryRange;

        /***********************************************************************

            Stack of the subdirectories that were postponed for visiting.

        ***********************************************************************/

        private SubdirectoryRange[]* stack;

        /***********************************************************************

            Current subdirectory that we're iterating.

        ***********************************************************************/

        private SubdirectoryRange item;

        /***********************************************************************

            Current directory entry where the range is pointing to.

        ***********************************************************************/

        private DirectoryEntry current_file;

        /***********************************************************************

            Indicator if the directory is empty, or we're passed after all
            entries.

        ***********************************************************************/

        private bool is_empty;

        /***********************************************************************

            Indicator if the range is still valid and if we may still use it.

        ***********************************************************************/

        private bool is_valid;

        /***********************************************************************

            Filename of the current element, as pointed by front();

        ***********************************************************************/

        private mstring* filename_buf;

        /***********************************************************************

            Set of reusable buffers to use.

        ***********************************************************************/

        private FileSystemBuffers* buffers;

        /***********************************************************************

            Delegate to call to get reusable buffer.

        ***********************************************************************/

        private void[]* delegate() getVoidBuffer;

        /***********************************************************************

            Resets the state and internal buffers to potentially reusable
            ones, using `getVoidBuffer`.

        ***********************************************************************/

        private void reset (void[]* delegate() getVoidBuffer)
        {
            this.getVoidBuffer = getVoidBuffer;
            this.path_buffer.reset(getVoidBuffer);
            this.stack = cast(typeof(this.stack))getVoidBuffer();
            this.filename_buf = cast(mstring*)getVoidBuffer();

            this.root = null;
            this.root_path = null;
            item = item.init;
            is_empty = false;
            this.current_file = DirectoryEntry.init;
        }

        /// ditto
        private void reset (FileSystemBuffers* buffers)
        {
            this.buffers = buffers;
            this.buffers.reset();

            this.path_buffer.reset(&buffers.path_buffer);
            this.stack = cast(typeof(this.stack))&buffers.stack_buffer;
            this.filename_buf = &buffers.filename_buffer;

            this.root = null;
            this.root_path = null;
            this.item = item.init;
            this.is_empty = false;
            this.current_file = DirectoryEntry.init;
            this.is_valid = true;
        }

        /***********************************************************************

            Returns:
                path relative to the top-level directory.

        ***********************************************************************/

        public cstring relativePath ()
        {
            return relative(*this.filename_buf, this.root_path);
        }

        /***********************************************************************

            Returns:
                the path of the current file

        ***********************************************************************/

        public cstring front ()
        {
            this.path_buffer.renderFilePath(this.current_file.name);
            (*this.filename_buf).copy(this.path_buffer.buffer);
            this.path_buffer.endRender(this.current_file.name);

            return *this.filename_buf;
        }

        /***********************************************************************

            Moves to the next element in range.

        ***********************************************************************/

        public void popFront ()
        {
            if (!this.is_valid) return;

            // find the next item to iterate over
            while (true)
            {
                // let's see where we at? Are we iterating over subitems
                // or over the file currently?

                // Avoid exceptions
                if (!this.item.isValid())
                {
                    this.is_valid = false;
                    return;
                }

                if (this.item.empty())
                {
                    if (!ocean.core.array.Mutation.pop(*this.stack, this.item))
                    {
                        this.is_empty = true;
                        return;
                    }
                    else
                    {
                        this.path_buffer.popPath();
                        assert (this.path_buffer.buffer.length >= this.root_path.length - 1);
                        continue;
                    }
                }
                else
                {
                    auto dir_entry = this.item.front.value;

                    if (!this.item.isValid())
                    {
                        this.is_valid = false;
                        return;
                    }

                    this.item.popFront();

                    // we need to check if this is a directory, or
                    // a file. If this is a file, we will point the
                    // iteration here and end with it, but if it's a directory,
                    // we need to descend into it.

                    if (!is_directory(dir_entry))
                    {
                        this.current_file = dir_entry;
                        return;
                    }
                    else
                    {
                        // push back parent directory to stack
                        *this.stack ~= this.item;
                        // iterate now over this directory
                        this.path_buffer.enterPath(dir_entry.name);

                        auto buf = this.buffers !is null?
                            cast(void[]*)&this.buffers.tree_buffer :
                            getVoidBuffer();

                        this.item = byKeyValue(dir_entry.children.entries, buf);
                        continue;
                    }
                }
            }

            assert (false);
        }

        /***********************************************************************

            Indicator if the range is still valid and its results may still
            be used. The range gets invalid when the underlying data structure
            gets changed. The check should be done after `popFront`.

        ***********************************************************************/

        public bool isValid ()
        {
            return this.is_valid;
        }

        /***********************************************************************

            Returns:
                true if the range is empty, or all elements were visited.

        ***********************************************************************/

        public bool empty ()
        {
            return this.is_empty;
        }


        /***********************************************************************

            Initialises the range.

        ***********************************************************************/

        private void start ()
        {
            // inside root directory. Push it to stack and move to the first
            // of it.
            auto buf = this.buffers !is null?
                cast(void[]*)&this.buffers.tree_buffer :
                getVoidBuffer();
            auto range = byKeyValue(this.root.children.entries, buf);

            this.item = range;
            if (this.item.empty)
            {
                this.is_empty = true;
                return;
            }

            this.path_buffer.enterPath(this.root_path);

            this.popFront();
            return;
        }
    }

    /***************************************************************************

        Returns:
            InputRange for iterating over the file-system cache.

    ***************************************************************************/

    public FileSystemIteratorRange traverse (void[]* delegate() getVoidBuffer)
    {
        // struct trick to new a new slice on a heap
        static struct Buffer { void[] data; }
        if (getVoidBuffer is null)
        {
            getVoidBuffer = { return &((new Buffer).data); };
        }

        FileSystemIteratorRange range;
        range.reset(getVoidBuffer);
        range.root = &this.root;
        range.root_path = this.root_path;
        range.start();

        return range;
    }

    public FileSystemIteratorRange traverse (FileSystemBuffers* buffers)
    {
        // struct trick to new a new slice on a heap
        FileSystemIteratorRange range;
        range.reset(buffers);
        range.root = &this.root;
        range.root_path = this.root_path;
        range.start();

        return range;

    }

    /***************************************************************************

        Does the inorder iteration over the file system. This calls a delegate
        for every directory entry in the file system.

        Params:
            path_buffer = path buffer to use to build the path in
            root = top-level directory
            dg = callback delegate

    ***************************************************************************/

    private void traverse (ref PathBuffer path_buffer,
        DirectoryEntry* root, void delegate(Const!(DirectoryEntry)* f, cstring rel_path) dg)
    {
        enforce(is_directory(*root));

        // Append this to the path
        path_buffer.enterPath(root.name);
        scope (exit) path_buffer.exitPath(root.name);

        // Call dg for the current dir
        dg(root, relative(path_buffer, this.root_path));


        // list all files
        foreach (file; root.children.entries)
        {
            if (is_directory(file))
            {
                traverse (path_buffer, &file, dg);
            }
            else
            {
                // temporarily build path with a filename
                path_buffer.renderFilePath(file.name);
                scope (exit) path_buffer.endRender(file.name);

                dg(&file, relative(path_buffer, this.root_path));
            }
        }
    }

    /***************************************************************************

        Does the inorder iteration over the file system. This calls a delegate
        for every directory entry in the file system.

        Params:
            dg = callback delegate

       Returns:
            true if the iterator is still valid (the underlying cache hasn't
            changed, false otherwise.

    ***************************************************************************/

    public bool traverse (void delegate(Const!(DirectoryEntry)* f, cstring rel_path) dg)
    {
        this.path_buffer.reset();
        this.path_buffer.enterPath(this.root_path);
        this.traverse(this.path_buffer, &this.root, dg);
        // TODO
        return true;
    }

    /***************************************************************************

        Adds new file into the cache.

        Params:
            filename = absolute path including the filename.

    ***************************************************************************/

    public void addNewFile(cstring filename)
    {
        getCreateOwnerDirectory(filename).addFile(basename(filename));
    }

    /***************************************************************************

        Deletes a file from the cache

        Params:
            filename = absolute path including the filename.

    ***************************************************************************/

    public void deleteFile(cstring filename)
    {
        getOwnerDirectory(filename).deleteFile(basename(filename));
    }

    /***************************************************************************

        Finds the lowest responsible directory for a file.

        Params:
            filename = absolute path of the file.

        Returns:
            the owner directory.

    ***************************************************************************/

    private DirectoryEntry* getOwnerDirectory (cstring filename)
    {
        enforce(this.root_path == filename[0..this.root_path.length]);
        filename = filename[this.root_path.length+1..$];

        DirectoryEntry* owner_dir = &this.root;

        chunkFilePath(filename,
            (cstring component, bool is_filename)
            {
                if (is_filename)
                {
                    filename = component;
                }
                else
                {
                    DirectoryName search_for;
                    search_for.name = component;
                    auto dir = owner_dir.children.entries.get(search_for);

                    if (dir && is_directory(*dir))
                    {
                        owner_dir = dir;
                    }
                    else
                    {
                        enforce(false, "No file in the directory.");
                    }
                }
            }
        );

        return owner_dir;
    }

    /***************************************************************************

        Finds and/or creates the lowest responsible directory for a file.

        Params:
            filename = absolute path of the file

        Returns:
            The owner directory.

    ***************************************************************************/

    private DirectoryEntry* getCreateOwnerDirectory (cstring filename)
    {
        enforce(this.root_path == filename[0..this.root_path.length]);
        filename = filename[this.root_path.length+1..$];

        DirectoryEntry* owner_dir = &this.root;

        chunkFilePath(filename,
            (cstring component, bool is_filename)
            {
                if (is_filename)
                {
                    filename = component;
                }
                else
                {
                    DirectoryName search_for;
                    search_for.name = component;
                    auto dir = owner_dir.children.entries.get(search_for);

                    if (dir && is_directory(*dir))
                    {
                        owner_dir = dir;
                    }
                    else
                    {
                        // We need to make one
                        auto new_dir = owner_dir.addSubdirectory(component);
                        owner_dir = new_dir;
                    }
                }
            }
        );

        return owner_dir;
    }


    unittest
    {
        // Tests the addNewFile
        auto instance = new FileSystemCache("root/");
        testThrown(instance.addNewFile("someOtherRoot/file"));
        instance.addNewFile("root/some_file");

        int count;
        instance.traverse((Const!(DirectoryEntry)* file, cstring rel_path)
        {
            if (rel_path != "")
            {
                test!("==")(rel_path, "some_file");
                test!("==")(file.name, "some_file");
            }
            else
            {
                test!("==")(file.name, "");
                test!("==")(is_directory(*file), true);
            }
            count++;
        });

        test!("==")(count, 2);

        // Test deletions
        testThrown(instance.deleteFile("root/hello/my_dir"));
        instance.deleteFile("root/some_file");

        count = 0;
        instance.traverse((Const!(DirectoryEntry)* file, cstring rel_path)
        {
            test!("==")(file.name, "");
            test!("==")(is_directory(*file), true);
            count++;
        });

        test!("==")(count, 1);

        // repeated deletions are fine
        instance.deleteFile("root/some_file");

        auto files_to_add = [
            "root/some_file",
            "root/directory/hello/hello/file",
            "root/directory/hello/blah",
            "root/some_other_directory/my_file"][];

        foreach (f; files_to_add)
        {
            instance.addNewFile(f);
        }

        cstring[] files_iterated;
        cstring[] dirs_iterated;

        instance.traverse((Const!(DirectoryEntry)* file, cstring rel_path)
        {
            if (!is_directory(*file))
            {
                files_iterated ~= "root/" ~ rel_path;
            }
            else
            {
                dirs_iterated ~= "root/" ~ rel_path;
            }
        });

        test!("==")(files_iterated.length, files_to_add.length);
        foreach (file; files_to_add)
        {
            test!("!=")(find(files_iterated, file), files_iterated.length);
        }

        test!("==")(dirs_iterated.length, 5);

        mstring buf;
        for (int i = 0; i < 10; i++)
        {
            for (int j = 0; j < 10; j++)
            {
                for (int k = 0; k < 1000; k++)
                {
                    buf.length = 0;
                    enableStomping(buf);
                    sformat(buf, "root/{}/{}/{}", i, j, k);
                    instance.addNewFile(buf);
                }
            }
        }

        // using strings for the literar comparasion
        // (i.e. 978989 should go be larger than 97889889)
        mstring previous_file_name;
        mstring previous_directory;
        mstring[] previous_path_chunks;

        // Test the traverse method
        instance.traverse((Const!(DirectoryEntry)* f, cstring rel_path) {
            if (f.name == "root/" || is_directory(*f))
                return;

            // if they are in the same directory, just compare
            // the file names
            if (previous_directory == rel_path)
            {
                test!("<")(previous_file_name, f.name);
            }

            previous_directory.copy(rel_path);
            previous_file_name.copy(f.name);
        });
    }
}
