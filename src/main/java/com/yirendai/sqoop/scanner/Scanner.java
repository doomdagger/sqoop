package com.yirendai.sqoop.scanner;

import java.io.*;
import java.util.List;
import java.util.Map;

/**
 * Created by lihe on 4/30/15.
 * @author Li He
 */
public abstract class Scanner {
    private String inputFile;
    private String outputFile;

    private BufferedReader reader;
    private BufferedWriter writer;

    public Scanner() {
    }

    public Scanner(String inputFile) {
        this.setInputFile(inputFile);
    }

    public Scanner(String inputFile, String outputFile) {
        this.setInputFile(inputFile);
        this.setOutputFile(outputFile);
    }

    public String getOutputFile() {
        return outputFile;
    }

    public void setOutputFile(String outputFile) {
        this.outputFile = outputFile;
    }

    public String getInputFile() {
        return inputFile;
    }

    public void setInputFile(String inputFile) {
        this.inputFile = inputFile;
    }

    public abstract void scan(List<Map<String, String>> configs) throws IOException;

    public BufferedWriter getWriter() throws FileNotFoundException {
        if (writer == null) {
            if (this.getOutputFile() == null || this.getOutputFile().equals("")) {
                writer = new BufferedWriter(
                        new OutputStreamWriter(System.out)
                );
            } else {
                writer = new BufferedWriter(
                        new OutputStreamWriter(new FileOutputStream(this.getOutputFile()))
                );
            }
        }

        return  writer;
    }

    public BufferedReader getReader() throws FileNotFoundException {
        if (reader == null) {
            reader = new BufferedReader(
                    new InputStreamReader(new FileInputStream(this.getInputFile()))
            );
        }

        return reader;
    }

    public void cleanup() throws IOException {
        if (reader != null) {
            reader.close();
        }
        if (writer != null) {
            writer.close();
        }
    }
}
