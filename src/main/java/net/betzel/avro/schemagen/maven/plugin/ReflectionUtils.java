package net.betzel.avro.schemagen.maven.plugin;

import org.apache.avro.util.ClassUtils;
import org.codehaus.plexus.util.StringUtils;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class ReflectionUtils {

    // ----------------------------------------------------------------------
    // Field utils
    // ----------------------------------------------------------------------

    public static Field getFieldByNameIncludingSuperclasses(String fieldName, Class<?> clazz) {
        Field result = null;
        try {
            result = clazz.getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            Class<?> superclass = clazz.getSuperclass();
            if (superclass != null) {
                result = getFieldByNameIncludingSuperclasses(fieldName, superclass);
            }
        }
        return result;
    }

    public static List<Field> getFieldsIncludingSuperclasses(Class<?> clazz) {
        List<Field> fields = new ArrayList(Arrays.asList(clazz.getDeclaredFields()));
        Class<?> superclass = clazz.getSuperclass();
        if (superclass != null) {
            fields.addAll(getFieldsIncludingSuperclasses(superclass));
        }
        return fields;
    }

    // ----------------------------------------------------------------------
    // Setter utils
    // ----------------------------------------------------------------------

    /**
     * Finds a setter in the given class for the given field. It searches interfaces and superclasses too.
     *
     * @param fieldName the name of the field (i.e. 'fooBar'); it will search for a method named 'setFooBar'.
     * @param clazz     The class to find the method in.
     * @return null or the method found.
     */
    public static Method getSetter(String fieldName, Class<?> clazz) {
        Method[] methods = clazz.getMethods();
        fieldName = "set" + StringUtils.capitalizeFirstLetter(fieldName);
        for (Method method : methods) {
            if (method.getName().equals(fieldName) && isSetter(method)) {
                return method;
            }
        }
        return null;
    }

    /**
     * Finds all setters in the given class and super classes.
     */
    public static List<Method> getSetters(Class<?> clazz) {
        Method[] methods = clazz.getMethods();
        List<Method> list = new ArrayList();
        for (Method method : methods) {
            if (isSetter(method)) {
                list.add(method);
            }
        }
        return list;
    }

    /**
     * Returns the class of the argument to the setter. Will throw an RuntimeException if the method isn't a setter.
     */
    public static Class<?> getSetterType(Method method) {
        if (!isSetter(method)) {
            throw new RuntimeException("The method " + method.getDeclaringClass().getName() + "." + method.getName() + " is not a setter.");
        }
        return method.getParameterTypes()[0];
    }

    // ----------------------------------------------------------------------
    // Value accesstors
    // ----------------------------------------------------------------------

    /**
     * attempts to set the value to the variable in the object passed in
     *
     * @param object
     * @param variable
     * @param value
     * @throws IllegalAccessException
     */
    public static void setVariableValueInObject(Object object, String variable, Object value) throws IllegalAccessException {
        Field field = getFieldByNameIncludingSuperclasses(variable, object.getClass());
        field.setAccessible(true);
        field.set(object, value);
    }

    /**
     * Generates a map of the fields and values on a given object, also pulls from superclasses
     *
     * @param object the object to generate the list of fields from
     * @return map containing the fields and their values
     */
    public static Object getValueIncludingSuperclasses(String variable, Object object) throws IllegalAccessException {
        Field field = getFieldByNameIncludingSuperclasses(variable, object.getClass());
        field.setAccessible(true);
        return field.get(object);
    }

    /**
     * Generates a map of the fields and values on a given object, also pulls from superclasses
     *
     * @param object the object to generate the list of fields from
     * @return map containing the fields and their values
     */
    public static Map<String, Object> getVariablesAndValuesIncludingSuperclasses(Object object) throws IllegalAccessException {
        Map<String, Object> map = new HashMap();
        gatherVariablesAndValuesIncludingSuperclasses(object, map);
        return map;
    }

    public static boolean isSetter(Method method) {
        return method.getReturnType().equals(Void.TYPE) && !Modifier.isStatic(method.getModifiers()) && method.getParameterTypes().length == 1;
    }

    /**
     * populates a map of the fields and values on a given object, also pulls from superclasses
     *
     * @param object the object to generate the list of fields from
     * @param map    to populate
     */
    private static void gatherVariablesAndValuesIncludingSuperclasses(Object object, Map<String, Object> map) throws IllegalAccessException {
        Class<?> clazz = object.getClass();
        Field[] fields = clazz.getDeclaredFields();
        AccessibleObject.setAccessible(fields, true);
        for (Field field : fields) {
            map.put(field.getName(), field.get(object));
        }
        Class<?> superclass = clazz.getSuperclass();
        if (!Object.class.equals(superclass)) {
            gatherVariablesAndValuesIncludingSuperclasses(superclass, map);
        }
    }

    public static boolean isClass(String variable) {
        try {
            ClassUtils.forName(variable);
        } catch (Exception ignored) {
            return false;
        }
        return true;
    }

}