package com.liweidao.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeywordUtil {
    public static List<String> analyze(String text) {
        StringReader sr = new StringReader(text);
        IKSegmenter ikSegmenter = new IKSegmenter(sr, true);
        Lexeme lex = null;
        ArrayList<String> arrayList = new ArrayList<>();
        while (true) {
            try {
                if ((lex = ikSegmenter.next()) != null) {
                    String lexemeText = lex.getLexemeText();
                    arrayList.add(lexemeText);
                } else {
                    break;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return arrayList;
    }

    public static void main(String[] args) {
        String text="Apple iPhoneXSMax (A2104) 256GB 深空灰色 移动联通电信4G手机 双卡双待";
        System.out.println(analyze(text));
    }
}
