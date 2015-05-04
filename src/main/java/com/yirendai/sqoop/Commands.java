package com.yirendai.sqoop;

import com.beust.jcommander.Parameter;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by lihe on 5/3/15.
 * @author Li He
 */
public class Commands {

    @Parameter
    public List<String> parameters = new ArrayList<String>();

    @Parameter(names = {"--help", "-h"}, description = "display help messages", help = true)
    public boolean help;

    @Parameter(names = { "--action", "-a" }, required = true, description = "the action to take:\n" +
            "\t\tworker - use the specified configuration file to communicate with sqoop server.\n" +
            "\t\tscanner - use the specified configuration file to generate ready-to-use configuration file for worker action.")
    public String action;

    @Parameter(names = {"--configuration", "-c"}, required = true, description = "the path of the configuration file, used by worker action or scanner action")
    public String conf;

    @Parameter(names = {"--input", "-i"}, description = "the path of the input file, only when scanner action is taken, this parameter is *required*")
    public String input;

    @Parameter(names = {"--output", "-o"}, description = "the path of the output file, omit this parameter will output will be directed to console")
    public String output;
}
