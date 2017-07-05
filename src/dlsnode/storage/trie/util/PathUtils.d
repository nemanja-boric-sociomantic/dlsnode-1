/// Utilities for the path manipulation
module dlsnode.storage.trie.util.PathUtils;

import ocean.transition;
import ocean.text.convert.Formatter;
import TextSearch = ocean.text.Search;
import ocean.core.array.Search;

version (UnitTest)
{
    import ocean.core.Test;
}

/// Buffer for building the file path while
/// traversing the tree. Use enterPath/exitPath
/// and renderFilePath/endRender methods to
/// generate the path while descending into
/// and out of subdirectories
struct PathBuffer
{
    import ocean.core.array.Mutation;

    private mstring* buffer_;

    /***************************************************************************

        Initialises the PathBuffer with it's own data buffer. Useful
        when the PathBuffer is longlived, so the data buffer can be
        contained inside it.

    ***************************************************************************/

    public void init ()
    {
        static struct S { mstring data; }
        this.buffer_ = &((new S).data);
    }

    /// Path buffer

    public cstring buffer ()
    {
        return *this.buffer_;
    }

    /// Resets the path instance
    public void reset (void[]* delegate() getVoidBuffer)
    {
        this.buffer_ = cast(mstring*)getVoidBuffer();
    }

    /// ditto
    public void reset ()
    {
        (*this.buffer_).length = 0;
        enableStomping(*this.buffer_);
    }

    /// ditto
    public void reset (mstring* buffer)
    {
        this.buffer_ = buffer;
    }


    /// Gets the current path
    public cstring data ()
    {
        return *this.buffer_;
    }

    /// Descents into subdirectory
    public void enterPath (cstring path)
    {
        if (path.length == 0)
            return;
        sformat(*this.buffer_, "{}/", path);
    }

    /// Makes the buffer contain the path of the
    /// file
    public void renderFilePath (cstring filename)
    {
        sformat(*this.buffer_, "{}", filename);
    }

    public void exitPath (cstring path)
    {
        if (path.length == 0)
            return;
        // +1 is for the / separator
        (*this.buffer_).length = (*this.buffer_).length - path.length - 1;
        enableStomping(*this.buffer_);
    }

    // pops the last component of the path
    public void popPath ()
    {
        // find the second last slash (this.buffer[$-1] is already a slash).
        auto last_slash = (*this.buffer_)[0..$-1].rfind('/');

        if (last_slash == (*this.buffer_).length-1)
            return;

        *this.buffer_ = (*this.buffer_)[0..last_slash+1];
        enableStomping(*this.buffer_);
    }

    public void endRender (cstring filename)
    {
        (*this.buffer_).length = (*this.buffer_).length - filename.length;
        enableStomping(*this.buffer_);
    }
}

/// Returns the path relative to `root`
public cstring relative (ref PathBuffer buffer, cstring root)
{
    return buffer.data[root.length+1..$];
}

/// ditto
public cstring relative (cstring absolute, cstring root)
{
    return absolute[root.length+1..$];
}

/// Returns the basename of the file
public cstring basename (cstring filename)
{
    auto last_slash = filename[0..$-1].rfind('/');

    if (last_slash == filename.length-1)
        return filename;

    return filename[last_slash+1..$];
}

/**************************************************************************

    Chunks the filename into a set of the paths.
    For example, `a/b/c/d/e` will cause dg to be called five times,
    with (a, false), (b, false), (c, false), (d, false), (e, true).

    Params:
        path = path to chunk
        dg = delegate to call for every path piece.

**************************************************************************/

public void chunkFilePath (cstring path,
    void delegate (cstring component, bool is_filename) dg)
{
    scope slash_search = TextSearch.SearchFruct!(Const!(char))("/");
    long slash_pos;

    // find the owner directory or create if missing
    do
    {
        if (path.length == 0)
        {
            return;
        }

        slash_pos = slash_search.forward(path);

        if (slash_pos == path.length)
        {
            dg (path, !!path.length);
            break;
        }

        auto component = path[0..slash_pos];
        path = path[slash_pos+1..$];

        dg (component, false);
    }
    while (slash_pos >= 0);
}

unittest
{
    void dont_call (cstring a, bool unused)
    {
        test!("==")(true, false);
    }
    chunkFilePath("", &dont_call);

    void only_filename (cstring filename, bool is_file)
    {
        test!("==")(is_file, true);
        test!("==")(filename, "filename");
    }
    chunkFilePath("filename", &only_filename);

    int counter = 0;
    void both_directory_and_file_name (cstring component, bool is_file)
    {
        counter++;
        if (is_file)
        {
            test!("==")(component, "filename");
        }
        else
        {
            test!("==")(component, "directory");
        }
    }

    chunkFilePath("directory/directory/directory/filename", &both_directory_and_file_name);
    test!("==")(counter, 4);


    void no_filename (cstring component, bool is_file)
    {
        test!("==")(component, "directory");
        test!("==")(is_file, false);
    }
    chunkFilePath("directory/directory/directory/", &no_filename);
}
