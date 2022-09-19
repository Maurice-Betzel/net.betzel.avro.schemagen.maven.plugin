package net.betzel.avro.schemagen.maven.plugin.test;

import net.betzel.avro.schemagen.maven.plugin.ReflectionUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReflectionUtilsTest {

    private Child child;
    private Parent parent;
    private Map<String, List<Type>> fieldBoundPolymorphicClassFiles;

    @Before
    public void init() {
        fieldBoundPolymorphicClassFiles = new HashMap(3);
        List<Type> polymorphicClassFiles = new ArrayList(3);
        polymorphicClassFiles.add(IOException.class);
        polymorphicClassFiles.add(NullPointerException.class);
        polymorphicClassFiles.add(IllegalArgumentException.class);
        fieldBoundPolymorphicClassFiles.put("net.betzel.avro.schemagen.maven.plugin.test.ReflectionUtilsTest.Child.wrapperLongField", polymorphicClassFiles);

        parent = new Parent();
        parent.primitiveField = 7;
        parent.stringField = "String field parent";
        parent.exception = new NullPointerException("Null pointer parent");

        child = new Child();
        child.primitiveField = 7;
        child.stringField = "String field child";
        child.exception = new NullPointerException("Null pointer child");
        child.wrapperLongField = Long.MAX_VALUE;
    }

    @Test
    public void testChild() {
        Field field = ReflectionUtils.getFieldByNameIncludingSuperclasses("stringField", Child.class);
        Assert.assertTrue(field.toGenericString().equals("java.lang.String net.betzel.avro.schemagen.maven.plugin.test.ReflectionUtilsTest$Child.stringField"));
    }

    @Test
    public void testParent() throws ClassNotFoundException {
        Field field = ReflectionUtils.getFieldByNameIncludingSuperclasses("primitiveField", Parent.class);
        Assert.assertTrue(field.toGenericString().equals("int net.betzel.avro.schemagen.maven.plugin.test.ReflectionUtilsTest$Parent.primitiveField"));
        field = ReflectionUtils.getFieldByNameIncludingSuperclasses("primitiveField", Child.class);
        Assert.assertTrue(field.toGenericString().equals("int net.betzel.avro.schemagen.maven.plugin.test.ReflectionUtilsTest$Parent.primitiveField"));

        List<Field> list = ReflectionUtils.getFieldsIncludingSuperclasses(Child.class);
        for(Field fld : list) {
            System.out.println("net.betzel.avro.schemagen.maven.plugin.test.ReflectionUtilsTest$Parent.primitiveField".endsWith(fld.getDeclaringClass().getName()+"."+ fld.getName()));
        }
        Assert.assertNotNull(list);
//        Object object = StaticJavaParser.parseType("net.betzel.avro.schemagen.maven.plugin.test.ReflectionUtilsTest$Parent.primitiveField");
//        Class clazzString = Class.forName("net.betzel.avro.schemagen.maven.plugin.test.ReflectionUtilsTest$Parent");
        //CodeGenerationUtils codeGenerationUtils;
//        field = ReflectionUtils.getFieldByNameIncludingSuperclasses("net.betzel.avro.schemagen.maven.plugin.test.ReflectionUtilsTest$Parent.primitiveField", Child.class);
        //Assert.assertTrue(field.toGenericString().equals("int net.betzel.avro.schemagen.maven.plugin.test.ReflectionUtilsTest$Parent.primitiveField"));
    }


    class Parent {
        int primitiveField;
        String stringField;
        Exception exception;
    }

    class Child extends Parent {
        String stringField;
        Long wrapperLongField;
    }

}
