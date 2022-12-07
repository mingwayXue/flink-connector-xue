package com.xue.bigdata.test.codegen;

import org.codehaus.commons.compiler.IScriptEvaluator;
import org.codehaus.janino.ExpressionEvaluator;
import org.codehaus.janino.ScriptEvaluator;

public class JaninoTest01 {
    public static void main(String[] args) throws Exception {
        String javaStr  = "System.out.println(\"hello JaninoTest01...\");";
        evaluateScript(javaStr);

        String expressStr = "res0 && (res1 || res2) && (1 > 2)";
        evaluateExpression(expressStr, new Object[]{true, false, true});
    }

    public static void evaluateExpression(String str, Object[] objects) throws Exception {
        IScriptEvaluator evaluator = new ExpressionEvaluator();
        String[] strings = new String[objects.length];
        Class[] classes = new Class[objects.length];
        for (int i = 0; i < objects.length; i++) {
            strings[i] = "res" + i;
            classes[i] = Boolean.class;
        }
        evaluator.setParameters(strings, classes);
        evaluator.cook(str);

        Object ret = evaluator.evaluate(objects);
        System.out.println(ret);
    }

    public static void evaluateScript(String str) throws Exception {
        IScriptEvaluator evaluator = new ScriptEvaluator();
        evaluator.cook(str);
        evaluator.evaluate(null);
    }
}
