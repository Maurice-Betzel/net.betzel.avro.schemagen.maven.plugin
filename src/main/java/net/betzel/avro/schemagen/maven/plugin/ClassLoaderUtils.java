package net.betzel.avro.schemagen.maven.plugin;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.Objects;

public class ClassLoaderUtils {

    /**
     * Show the class loader hierarchy for this class. Uses default line break and
     * tab text characters.
     *
     * @param obj  object to analyze loader hierarchy for
     * @param role a description of the role of this class in the application (e.g.,
     *             "servlet" or "EJB reference")
     * @return a String showing the class loader hierarchy for this class
     */
    public static String showClassLoaderHierarchy(Object obj, String role) {
        return showClassLoaderHierarchy(obj, role, "\n", "\t");
    }

    /**
     * Show the class loader hierarchy for this class.
     *
     * @param obj       object to analyze loader hierarchy for
     * @param role      a description of the role of this class in the application (e.g.,
     *                  "servlet" or "EJB reference")
     * @param lineBreak line break
     * @param tabText   text to use to set tabs
     * @return a String showing the class loader hierarchy for this class
     */
    public static String showClassLoaderHierarchy(Object obj, String role, String lineBreak, String tabText) {
        String s = "object of " + obj.getClass() + ": role is " + role + lineBreak;
        return s + showClassLoaderHierarchy(obj.getClass().getClassLoader(), lineBreak, tabText, 0);
    }

    /**
     * Show the class loader hierarchy for the given class loader. Uses default line
     * break and tab text characters.
     *
     * @param cl class loader to analyze hierarchy for
     * @return a String showing the class loader hierarchy for this class
     */
    public static String showClassLoaderHierarchy(ClassLoader cl) {
        return showClassLoaderHierarchy(cl, "\n", "\t");
    }

    /**
     * Show the class loader hierarchy for the given class loader.
     *
     * @param cl        class loader to analyze hierarchy for
     * @param lineBreak line break
     * @param tabText   text to use to set tabs
     * @return a String showing the class loader hierarchy for this class
     */
    public static String showClassLoaderHierarchy(ClassLoader cl, String lineBreak, String tabText) {
        return showClassLoaderHierarchy(cl, lineBreak, tabText, 0);
    }

    /**
     * Show the class loader hierarchy for the given class loader.
     *
     * @param classLoader class loader to analyze hierarchy for
     * @param lineBreak   line break
     * @param tabText     text to use to set tabs
     * @param indent      nesting level (from 0) of this loader; used in pretty printing
     * @return a String showing the class loader hierarchy for this class
     */
    private static String showClassLoaderHierarchy(ClassLoader classLoader, String lineBreak, String tabText, int indent) {
        StringBuffer countedIdent = new StringBuffer();
        for (int i = 0; i < indent; i++) {
            countedIdent.append(tabText);
        }
        String currentIdent = countedIdent.toString();

        if (Objects.isNull(classLoader)) {
            ClassLoader ccl = Thread.currentThread().getContextClassLoader();
            return currentIdent + "context class loader=[" + ccl + "] hashCode=" + ccl.hashCode();
        }

        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append(currentIdent).append("[").append(classLoader).append("] hashCode=").append(classLoader.hashCode()).append(lineBreak);

        stringBuffer.append(currentIdent).append("-> loader resource: ").append(classLoader.getResource("/")).append(lineBreak);
        try {
            // showing 'directories'
            Enumeration<URL> resources = classLoader.getResources(".");
            while (resources.hasMoreElements())
                stringBuffer.append(currentIdent).append("-> loader resource element: ").append(resources.nextElement()).append(lineBreak);
            // showing 'jar files'
            resources = classLoader.getResources("META-INF");
            while (resources.hasMoreElements())
                stringBuffer.append(currentIdent).append("-> loader resource element: ").append(resources.nextElement()).append(lineBreak);
        } catch (IOException ioe) {
            stringBuffer.append(currentIdent).append("-> error: can't get resource elements: ").append(ioe.getMessage()).append(lineBreak);
        }
        ClassLoader parent = classLoader.getParent();
        return stringBuffer + showClassLoaderHierarchy(parent, lineBreak, tabText, indent + 1);
    }

}