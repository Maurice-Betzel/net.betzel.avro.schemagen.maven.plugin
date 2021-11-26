package net.betzel.avro.schemagen.maven.plugin;

import org.apache.commons.collections4.iterators.IteratorEnumeration;
import org.apache.commons.io.IOUtils;
import sun.net.www.ParseUtil;
import sun.security.util.SecurityConstants;

import java.io.*;
import java.net.*;
import java.security.*;
import java.util.*;
import java.util.jar.Attributes;
import java.util.jar.Attributes.Name;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import static java.security.AccessController.doPrivileged;
import static java.util.jar.JarFile.MANIFEST_NAME;
import static org.apache.commons.collections4.IteratorUtils.*;

/**
 * This implements {@link SecureClassLoader} and creates its class path over a given collection of jarFiles or directories. This implements also {@link Closeable}.
 */
public class FileClassLoader extends SecureClassLoader implements Closeable {

    private final AccessControlContext _acc = AccessController.getContext();
    private final Collection<JarFile> _jarFiles;
    private final Collection<File> _directories;

    private volatile boolean _closed;

    public FileClassLoader(File classPath) throws IOException {
        this(Collections.singleton(classPath), null);
    }

    public FileClassLoader(Iterable<File> classPath) throws IOException {
        this(classPath, null);
    }

    public FileClassLoader(File classPath, ClassLoader parent) throws IOException {
        this(Collections.singleton(classPath), parent);
    }

    public FileClassLoader(Iterable<File> classPath, ClassLoader parent) throws IOException {
        super(parent);
        if (classPath == null) {
            throw new NullPointerException("The parameter files is null.");
        }
        final Collection<JarFile> jarFiles = new ArrayList();
        final Collection<File> directories = new ArrayList();
        for (File classPathPart : classPath) {
            if (classPathPart.isDirectory()) {
                directories.add(classPathPart);
            } else {
                final JarFile jarFile = new JarFile(classPathPart);
                jarFiles.add(jarFile);
            }
        }
        _jarFiles = Collections.unmodifiableCollection(jarFiles);
        _directories = Collections.unmodifiableCollection(directories);
    }

    @Override
    public URL getResource(String name) {
        final URL ourResources = findResource(name);
        final URL result;
        if (ourResources != null) {
            result = ourResources;
        } else {
            final ClassLoader parent = getParent();
            result = parent != null ? parent.getResource(name) : null;
        }
        return result;
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        final Enumeration<URL> localResources = findResources(name);
        final ClassLoader parent = getParent();
        // noinspection unchecked
        final Enumeration<URL> globalResources = parent != null ? parent.getResources(name) : asEnumeration(emptyIterator());
        return new Enumeration<URL>() {

            private boolean _local = true;

            @Override
            public boolean hasMoreElements() {
                if (_local && !localResources.hasMoreElements()) {
                    _local = false;
                }
                return _local ? localResources.hasMoreElements() : globalResources.hasMoreElements();
            }

            @Override
            public URL nextElement() {
                if (_local && localResources.nextElement() == null) {
                    _local = false;
                }
                return _local ? localResources.nextElement() : globalResources.nextElement();
            }
        };
    }

    @Override
    public synchronized void close() throws IOException {
        if (!_closed) {
            _closed = true;
            for (JarFile jarFile : _jarFiles) {
                jarFile.close();
            }
        }
    }

    public boolean isClosed() {
        return _closed;
    }

    @Override
    protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        Class<?> result = findLoadedClass(name);
        if (result == null) {
            try {
                result = findClass(name);
            } catch (ClassNotFoundException ignored) {
                final ClassLoader parent = getParent();
                if (parent != null) {
                    result = parent.loadClass(name);
                }
            }
        }
        if (resolve) {
            resolveClass(result);
        }
        return result;
    }

    @Override
    protected synchronized Class<?> findClass(final String name) throws ClassNotFoundException {
        ensureNotClosed();
        try {
            return doPrivileged(new PrivilegedExceptionAction<Class<?>>() {
                @Override
                public Class<?> run() throws Exception {
                    Class<?> result;
                    result = null;
                    final String path = name.replace('.', '/').concat(".class");
                    final Iterator<File> i = _directories.iterator();
                    while (result == null && i.hasNext()) {
                        final File directory = i.next();
                        final File file = new File(directory, path);
                        if (file.isFile()) {
                            result = defineClass(name, new DirectoryResource(directory, file));
                        }
                    }
                    if (result == null) {
                        final Iterator<JarFile> j = _jarFiles.iterator();
                        while (result == null && j.hasNext()) {
                            final JarFile jarFile = j.next();
                            final JarEntry jarEntry = jarFile.getJarEntry(path);
                            if (jarEntry != null) {
                                result = defineClass(name, new JarResource(jarFile, jarEntry));
                            }
                        }
                    }
                    if (result == null) {
                        throw new ClassNotFoundException(name);
                    }
                    return result;
                }
            }, _acc);
        } catch (PrivilegedActionException e) {
            final Exception exception = e.getException();
            if (exception instanceof ClassNotFoundException) {
                throw (ClassNotFoundException) exception;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    private Class<?> defineClass(String name, Resource resource) throws IOException {
        final int i = name.lastIndexOf('.');
        final URL packageUrl = resource.getPackageUrl();
        if (i != -1) {
            final String packageName = name.substring(0, i);
            // Check if package already loaded.
            final Package pkg = getPackage(packageName);
            final Manifest man = resource.getManifest();
            if (pkg != null) {
                // Package found, so check package sealing.
                if (pkg.isSealed()) {
                    // Verify that code source URL is the same.
                    if (!pkg.isSealed(packageUrl)) {
                        throw new SecurityException("sealing violation: package " + packageName + " is sealed");
                    }
                } else {
                    // Make sure we are not attempting to seal the package
                    // at this code source URL.
                    if ((man != null) && isSealed(packageName, man)) {
                        throw new SecurityException(
                                "sealing violation: can't seal package " + packageName + ": already loaded");
                    }
                }
            } else {
                if (man != null) {
                    definePackage(packageName, man, packageUrl);
                } else {
                    definePackage(packageName, null, null, null, null, null, null, null);
                }
            }
        }
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        final InputStream inputStream = resource.openStream();
        try {
            IOUtils.copy(inputStream, byteArrayOutputStream);
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
        final byte[] bytes = byteArrayOutputStream.toByteArray();
        final CodeSigner[] signers = resource.getCodeSigners();
        final CodeSource cs = new CodeSource(packageUrl, signers);
        return defineClass(name, bytes, 0, bytes.length, new ProtectionDomain(cs, new Permissions()));
    }

    @Override
    protected synchronized URL findResource(final String name) {
        ensureNotClosed();
        return doPrivileged(new PrivilegedAction<URL>() {
            @Override
            public URL run() {
                URL result = null;
                final Iterator<File> i = _directories.iterator();
                while (result == null && i.hasNext()) {
                    final File directory = i.next();
                    final File file = new File(directory, name);
                    if (file.isFile()) {
                        result = new DirectoryResource(directory, file).getResourceUrl();
                    }
                }
                if (result == null) {
                    final Iterator<JarFile> j = _jarFiles.iterator();
                    while (result == null && j.hasNext()) {
                        final JarFile jarFile = j.next();
                        final JarEntry jarEntry = jarFile.getJarEntry(name);
                        if (jarEntry != null) {
                            result = new JarResource(jarFile, jarEntry).getResourceUrl();
                        }
                    }
                }
                return result;
            }
        }, _acc);
    }

    @Override
    protected synchronized Enumeration<URL> findResources(final String name) throws IOException {
        ensureNotClosed();
        final Iterable<URL> iterable = doPrivileged(new PrivilegedAction<Iterable<URL>>() {
            @Override
            public Iterable<URL> run() {
                final Collection<URL> result = new ArrayList<URL>();
                for (File directory : _directories) {
                    final File file = new File(directory, name);
                    if (file.isFile()) {
                        result.add(new DirectoryResource(directory, file).getResourceUrl());
                    }
                }
                for (JarFile jarFile : _jarFiles) {
                    final JarEntry jarEntry = jarFile.getJarEntry(name);
                    if (jarEntry != null) {
                        result.add(new JarResource(jarFile, jarEntry).getResourceUrl());
                    }
                }
                return result;
            }
        }, _acc);
        //noinspection unchecked
        return new IteratorEnumeration(iterable.iterator());
    }

    protected synchronized void ensureNotClosed() {
        if (_closed) {
            throw new IllegalStateException(this + " is already closed.");
        }
    }

    /**
     * This is a copy of {@link URLClassLoader#getPermissions(CodeSource)}.
     *
     * Returns the permissions for the given codesource object.
     * The implementation of this method first calls super.getPermissions
     * and then adds permissions based on the URL of the codesource.
     * <p>
     * If the protocol of this URL is "jar", then the permission granted
     * is based on the permission that is required by the URL of the Jar
     * file.
     * <p>
     * If the protocol is "file"
     * and the path specifies a file, then permission to read that
     * file is granted. If protocol is "file" and the path is
     * a directory, permission is granted to read all files
     * and (recursively) all files and subdirectories contained in
     * that directory.
     * <p>
     * If the protocol is not "file", then
     * to connect to and accept connections from the URL's host is granted.
     * @param codesource the codesource
     * @return the permissions granted to the codesource
     */
    @Override
    protected PermissionCollection getPermissions(CodeSource codesource) {
        final PermissionCollection perms = super.getPermissions(codesource);
        final URL url = codesource.getLocation();
        Permission p;
        URLConnection urlConnection;
        try {
            urlConnection = url.openConnection();
            p = urlConnection.getPermission();
        } catch (IOException ignored) {
            p = null;
            urlConnection = null;
        }
        if (p instanceof FilePermission) {
            // if the permission has a separator char on the end,
            // it means the codebase is a directory, and we need
            // to add an additional permission to read recursively
            String path = p.getName();
            if (path.endsWith(File.separator)) {
                path += "-";
                p = new FilePermission(path, SecurityConstants.FILE_READ_ACTION);
            }
        } else if ((p == null) && (url.getProtocol().equals("file"))) {
            String path = url.getFile().replace('/', File.separatorChar);
            path = ParseUtil.decode(path);
            if (path.endsWith(File.separator)) {
                path += "-";
            }
            p = new FilePermission(path, SecurityConstants.FILE_READ_ACTION);
        } else {
            URL locUrl = url;
            if (urlConnection instanceof JarURLConnection) {
                locUrl = ((JarURLConnection) urlConnection).getJarFileURL();
            }
            final String host = locUrl.getHost();
            if (host != null && (host.length() > 0)) {
                p = new SocketPermission(host, SecurityConstants.SOCKET_CONNECT_ACCEPT_ACTION);
            }
        }
        // make sure the person that created this class loader
        // would have this permission

        if (p != null) {
            final SecurityManager sm = System.getSecurityManager();
            if (sm != null) {
                final Permission fp = p;
                doPrivileged(new PrivilegedAction<Void>() {
                    @Override
                    public Void run() throws SecurityException {
                        sm.checkPermission(fp);
                        return null;
                    }
                }, _acc);
            }
            perms.add(p);
        }
        return perms;
    }

    /*
     * This is a copy of {@link URLClassLoader#getPermissions(CodeSource)}.
     *
     * Returns true if the specified package name is sealed according to the
     * given manifest.
     */
    private boolean isSealed(String name, Manifest man) {
        final String path = name.replace('.', '/').concat("/");
        Attributes attr = man.getAttributes(path);
        String sealed = null;
        if (attr != null) {
            sealed = attr.getValue(Name.SEALED);
        }
        if (sealed == null) {
            if ((attr = man.getMainAttributes()) != null) {
                sealed = attr.getValue(Name.SEALED);
            }
        }
        return "true".equalsIgnoreCase(sealed);
    }

    /**
     * This is a copy of {@link URLClassLoader#getPermissions(CodeSource)}.
     *
     * Defines a new package by name in this ClassLoader. The attributes contained in the specified Manifest will be used to obtain package version and sealing
     * information. For sealed packages, the additional URL specifies the code source URL from which the package was loaded.
     *
     * @param name the package name
     * @param man the Manifest containing package version and sealing information
     * @param url the code source url for the package, or null if none
     * @return the newly defined Package object
     * @throws IllegalArgumentException if the package name duplicates an existing package either in this class loader or one of its ancestors
     */
    protected Package definePackage(String name, Manifest man, URL url) throws IllegalArgumentException {
        final String path = name.replace('.', '/').concat("/");
        String specTitle = null;
        String specVersion = null;
        String specVendor = null;
        String implTitle = null;
        String implVersion = null;
        String implVendor = null;
        String sealed = null;
        URL sealBase = null;

        Attributes attr = man.getAttributes(path);
        if (attr != null) {
            specTitle = attr.getValue(Name.SPECIFICATION_TITLE);
            specVersion = attr.getValue(Name.SPECIFICATION_VERSION);
            specVendor = attr.getValue(Name.SPECIFICATION_VENDOR);
            implTitle = attr.getValue(Name.IMPLEMENTATION_TITLE);
            implVersion = attr.getValue(Name.IMPLEMENTATION_VERSION);
            implVendor = attr.getValue(Name.IMPLEMENTATION_VENDOR);
            sealed = attr.getValue(Name.SEALED);
        }
        attr = man.getMainAttributes();
        if (attr != null) {
            if (specTitle == null) {
                specTitle = attr.getValue(Name.SPECIFICATION_TITLE);
            }
            if (specVersion == null) {
                specVersion = attr.getValue(Name.SPECIFICATION_VERSION);
            }
            if (specVendor == null) {
                specVendor = attr.getValue(Name.SPECIFICATION_VENDOR);
            }
            if (implTitle == null) {
                implTitle = attr.getValue(Name.IMPLEMENTATION_TITLE);
            }
            if (implVersion == null) {
                implVersion = attr.getValue(Name.IMPLEMENTATION_VERSION);
            }
            if (implVendor == null) {
                implVendor = attr.getValue(Name.IMPLEMENTATION_VENDOR);
            }
            if (sealed == null) {
                sealed = attr.getValue(Name.SEALED);
            }
        }
        if ("true".equalsIgnoreCase(sealed)) {
            sealBase = url;
        }
        return definePackage(name, specTitle, specVersion, specVendor, implTitle, implVersion, implVendor,
                sealBase);
    }

    protected static abstract class Resource {

        public abstract InputStream openStream() throws IOException;

        public abstract URL getResourceUrl();

        public abstract URL getPackageUrl();

        public abstract Manifest getManifest() throws IOException;

        public abstract CodeSigner[] getCodeSigners() throws IOException;
    }

    protected static class JarResource extends Resource {

        private final JarFile _jarFile;
        private final JarEntry _jarEntry;

        protected JarResource(JarFile jarFile, JarEntry jarEntry) {
            if (jarFile == null) {
                throw new NullPointerException();
            }
            if (jarEntry == null) {
                throw new NullPointerException();
            }
            _jarFile = jarFile;
            _jarEntry = jarEntry;
        }

        @Override
        public InputStream openStream() throws IOException {
            return _jarFile.getInputStream(_jarEntry);
        }

        @Override
        public URL getResourceUrl() {
            try {
                return new URL("jar", "", -1, new File(_jarFile.getName()).toURI() + "!/" + _jarEntry.getName(),
                        new URLStreamHandler() {
                            @Override
                            protected URLConnection openConnection(URL url) throws IOException {
                                return new URLConnection(url) {
                                    @Override
                                    public InputStream getInputStream() throws IOException {
                                        return _jarFile.getInputStream(_jarEntry);
                                    }

                                    @Override
                                    public void connect() throws IOException {
                                    }
                                };
                            }
                        });
            } catch (MalformedURLException e) {
                throw new RuntimeException("Could not construct a url for " + _jarFile + " and " + _jarEntry + ".",
                        e);
            }
        }

        @Override
        public URL getPackageUrl() {
            try {
                return new File(_jarFile.getName()).toURI().toURL();
            } catch (MalformedURLException e) {
                throw new RuntimeException("Could not construct a url for " + _jarFile + ".", e);
            }
        }

        @Override
        public Manifest getManifest() throws IOException {
            return _jarFile.getManifest();
        }

        @Override
        public CodeSigner[] getCodeSigners() throws IOException {
            return _jarEntry.getCodeSigners();
        }
    }

    protected static class DirectoryResource extends Resource {

        private final File _directory;
        private final File _file;

        protected DirectoryResource(File directory, File file) {
            if (directory == null) {
                throw new NullPointerException();
            }
            if (file == null) {
                throw new NullPointerException();
            }
            _directory = directory;
            _file = file;
        }

        @Override
        public InputStream openStream() throws IOException {
            return new FileInputStream(_file);
        }

        @Override
        public URL getResourceUrl() {
            try {
                return _file.toURI().toURL();
            } catch (MalformedURLException e) {
                throw new RuntimeException("Could not construct a url for " + _file + ".", e);
            }
        }

        @Override
        public URL getPackageUrl() {
            try {
                return new URL(_directory.toURI().toURL(), "", new URLStreamHandler() {
                    @Override
                    protected URLConnection openConnection(URL url) throws IOException {
                        return new URLConnection(url) {
                            @Override
                            public void connect() throws IOException {
                            }
                        };
                    }
                });
            } catch (MalformedURLException e) {
                throw new RuntimeException("Could not construct a url for " + _directory + ".", e);
            }
        }

        @Override
        public Manifest getManifest() throws IOException {
            final File manifestFile = new File(_directory, MANIFEST_NAME);
            final Manifest manifest;
            if (manifestFile.isFile()) {
                final FileInputStream is = new FileInputStream(manifestFile);
                try {
                    manifest = new Manifest(is);
                } finally {
                    IOUtils.closeQuietly(is);
                }
            } else {
                manifest = null;
            }
            return manifest;
        }

        @Override
        public CodeSigner[] getCodeSigners() throws IOException {
            return null;
        }
    }
}