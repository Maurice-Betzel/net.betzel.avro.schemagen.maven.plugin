package net.betzel.avro.schemagen.maven.plugin;

import sun.net.www.ParseUtil;
import sun.security.util.SecurityConstants;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilePermission;
import java.io.IOException;
import java.io.InputStream;
import java.net.JarURLConnection;
import java.net.MalformedURLException;
import java.net.SocketPermission;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.CodeSigner;
import java.security.CodeSource;
import java.security.Permission;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.ProtectionDomain;
import java.security.SecureClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Objects;
import java.util.jar.Attributes;
import java.util.jar.Attributes.Name;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import static java.security.AccessController.doPrivileged;
import static java.util.jar.JarFile.MANIFEST_NAME;

/**
 * This implements {@link SecureClassLoader} and creates its class path over a given collection of jarFiles or directories. This implements also {@link Closeable}.
 */
public class FileClassLoader extends SecureClassLoader implements Closeable {

    private AccessControlContext accessControlContext = AccessController.getContext();
    private Collection<JarFile> jarFiles;
    private Collection<File> directories;

    private volatile boolean closed;

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
        Objects.requireNonNull(classPath, "The parameter files is missing");
        Collection<JarFile> jarFiles = new ArrayList();
        Collection<File> directories = new ArrayList();
        for (File classPathPart : classPath) {
            if (classPathPart.isDirectory()) {
                directories.add(classPathPart);
            } else {
                JarFile jarFile = new JarFile(classPathPart);
                jarFiles.add(jarFile);
            }
        }
        this.jarFiles = Collections.unmodifiableCollection(jarFiles);
        this.directories = Collections.unmodifiableCollection(directories);
    }

    @Override
    public URL getResource(String name) {
        URL ourResources = findResource(name);
        URL result;
        if (Objects.nonNull(ourResources)) {
            result = ourResources;
        } else {
            ClassLoader parent = getParent();
            result = Objects.nonNull(parent) ? parent.getResource(name) : null;
        }
        return result;
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        Enumeration<URL> localResources = findResources(name);
        ClassLoader parent = getParent();
        Enumeration<URL> globalResources = Objects.nonNull(parent) ? parent.getResources(name) : Collections.emptyEnumeration();
        return new Enumeration<URL>() {

            private boolean local = true;

            @Override
            public boolean hasMoreElements() {
                if (local && !localResources.hasMoreElements()) {
                    local = false;
                }
                return local ? localResources.hasMoreElements() : globalResources.hasMoreElements();
            }

            @Override
            public URL nextElement() {
                if (local && Objects.isNull(localResources.nextElement())) {
                    local = false;
                }
                return local ? localResources.nextElement() : globalResources.nextElement();
            }
        };
    }

    @Override
    public synchronized void close() throws IOException {
        if (!closed) {
            closed = true;
            for (JarFile jarFile : jarFiles) {
                jarFile.close();
            }
        }
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        Class<?> result = findLoadedClass(name);
        if (Objects.isNull(result)) {
            try {
                result = findClass(name);
            } catch (ClassNotFoundException ignored) {
                ClassLoader parent = getParent();
                if (Objects.nonNull(parent)) {
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
    protected synchronized Class<?> findClass(String name) throws ClassNotFoundException {
        ensureNotClosed();
        try {
            return doPrivileged(new PrivilegedExceptionAction<Class<?>>() {
                @Override
                public Class<?> run() throws Exception {
                    Class<?> result;
                    result = null;
                    String path = name.replace('.', '/').concat(".class");
                    Iterator<File> i = directories.iterator();
                    while (Objects.isNull(result) && i.hasNext()) {
                        File directory = i.next();
                        File file = new File(directory, path);
                        if (file.isFile()) {
                            result = defineClass(name, new DirectoryResource(directory, file));
                        }
                    }
                    if (Objects.isNull(result)) {
                        Iterator<JarFile> j = jarFiles.iterator();
                        while (Objects.isNull(result) && j.hasNext()) {
                            JarFile jarFile = j.next();
                            JarEntry jarEntry = jarFile.getJarEntry(path);
                            if (jarEntry != null) {
                                result = defineClass(name, new JarResource(jarFile, jarEntry));
                            }
                        }
                    }
                    if (Objects.isNull(result)) {
                        throw new ClassNotFoundException(name);
                    }
                    return result;
                }
            }, accessControlContext);
        } catch (PrivilegedActionException e) {
            Exception exception = e.getException();
            if (exception instanceof ClassNotFoundException) {
                throw (ClassNotFoundException) exception;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    private Class<?> defineClass(String name, Resource resource) throws IOException {
        int i = name.lastIndexOf('.');
        URL packageUrl = resource.getPackageUrl();
        if (i != -1) {
            String packageName = name.substring(0, i);
            // Check if package already loaded.
            Package pkg = getPackage(packageName);
            Manifest man = resource.getManifest();
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
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(); InputStream inputStream = resource.openStream()) {
            byte[] buffer = new byte[8192];
            int n;
            while (-1 != (n = inputStream.read(buffer))) {
                byteArrayOutputStream.write(buffer, 0, n);
            }
            byte[] bytes = byteArrayOutputStream.toByteArray();
            CodeSigner[] signers = resource.getCodeSigners();
            CodeSource cs = new CodeSource(packageUrl, signers);
            return defineClass(name, bytes, 0, bytes.length, new ProtectionDomain(cs, new Permissions()));
        }
    }

    @Override
    protected synchronized URL findResource(String name) {
        ensureNotClosed();
        return doPrivileged(new PrivilegedAction<URL>() {
            @Override
            public URL run() {
                URL result = null;
                Iterator<File> i = directories.iterator();
                while (Objects.isNull(result) && i.hasNext()) {
                    File directory = i.next();
                    File file = new File(directory, name);
                    if (file.isFile()) {
                        result = new DirectoryResource(directory, file).getResourceUrl();
                    }
                }
                if (Objects.isNull(result)) {
                    Iterator<JarFile> j = jarFiles.iterator();
                    while (result == null && j.hasNext()) {
                        JarFile jarFile = j.next();
                        JarEntry jarEntry = jarFile.getJarEntry(name);
                        if (jarEntry != null) {
                            result = new JarResource(jarFile, jarEntry).getResourceUrl();
                        }
                    }
                }
                return result;
            }
        }, accessControlContext);
    }

    @Override
    protected synchronized Enumeration<URL> findResources(String name) throws IOException {
        ensureNotClosed();
        Iterable<URL> iterable = doPrivileged(new PrivilegedAction<Iterable<URL>>() {
            @Override
            public Iterable<URL> run() {
                Collection<URL> result = new ArrayList<URL>();
                for (File directory : directories) {
                    File file = new File(directory, name);
                    if (file.isFile()) {
                        result.add(new DirectoryResource(directory, file).getResourceUrl());
                    }
                }
                for (JarFile jarFile : jarFiles) {
                    JarEntry jarEntry = jarFile.getJarEntry(name);
                    if (jarEntry != null) {
                        result.add(new JarResource(jarFile, jarEntry).getResourceUrl());
                    }
                }
                return result;
            }
        }, accessControlContext);
        //noinspection unchecked
        return new IteratorEnumeration(iterable.iterator());
    }

    protected synchronized void ensureNotClosed() {
        if (closed) {
            throw new IllegalStateException(this + " is already closed.");
        }
    }

    /**
     * This is a copy of {@link URLClassLoader#getPermissions(CodeSource)}.
     * <p>
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
     *
     * @param codesource the codesource
     * @return the permissions granted to the codesource
     */
    @Override
    protected PermissionCollection getPermissions(CodeSource codesource) {
        PermissionCollection perms = super.getPermissions(codesource);
        URL url = codesource.getLocation();
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
            String host = locUrl.getHost();
            if (host != null && (host.length() > 0)) {
                p = new SocketPermission(host, SecurityConstants.SOCKET_CONNECT_ACCEPT_ACTION);
            }
        }
        // make sure the person that created this class loader
        // would have this permission

        if (p != null) {
            SecurityManager sm = System.getSecurityManager();
            if (sm != null) {
                Permission fp = p;
                doPrivileged(new PrivilegedAction<Void>() {
                    @Override
                    public Void run() throws SecurityException {
                        sm.checkPermission(fp);
                        return null;
                    }
                }, accessControlContext);
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
        String path = name.replace('.', '/').concat("/");
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
     * <p>
     * Defines a new package by name in this ClassLoader. The attributes contained in the specified Manifest will be used to obtain package version and sealing
     * information. For sealed packages, the additional URL specifies the code source URL from which the package was loaded.
     *
     * @param name the package name
     * @param man  the Manifest containing package version and sealing information
     * @param url  the code source url for the package, or null if none
     * @return the newly defined Package object
     * @throws IllegalArgumentException if the package name duplicates an existing package either in this class loader or one of its ancestors
     */
    protected Package definePackage(String name, Manifest man, URL url) throws IllegalArgumentException {
        String path = name.replace('.', '/').concat("/");
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

        private JarFile _jarFile;
        private JarEntry _jarEntry;

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

        private File _directory;
        private File _file;

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
            File manifestFile = new File(_directory, MANIFEST_NAME);
            Manifest manifest;
            if (manifestFile.isFile()) {
                try (FileInputStream fileInputStream = new FileInputStream(manifestFile)) {
                    manifest = new Manifest(fileInputStream);
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