package org.fao.ess.cache.d3s;

import org.apache.commons.codec.digest.DigestUtils;

import java.io.File;
import java.io.FileInputStream;

public class CreateMD5 {

    public static void main(String[] args) throws Exception {
        File file = new File("jsExamples/2.0/CodeList/Test/CodesFilter1.json");

        if (file.exists() && file.isFile()) {
            System.out.println(DigestUtils.md5Hex(new FileInputStream(file)));
        }
    }

}