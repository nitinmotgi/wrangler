/*
 * Copyright © 2016-2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.wrangler.parser;

import co.cask.wrangler.api.Directive;
import co.cask.wrangler.api.DirectiveContext;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.RecipeParser;
import co.cask.wrangler.api.RecipePipeline;
import co.cask.wrangler.executor.ICDCatalog;
import co.cask.wrangler.steps.IncrementTransientVariable;
import co.cask.wrangler.steps.SetTransientVariable;
import co.cask.wrangler.steps.date.DiffDate;
import co.cask.wrangler.steps.date.FormatDate;
import co.cask.wrangler.steps.language.SetCharset;
import co.cask.wrangler.steps.nlp.Stemming;
import co.cask.wrangler.steps.parser.CsvParser;
import co.cask.wrangler.steps.parser.FixedLengthParser;
import co.cask.wrangler.steps.parser.HL7Parser;
import co.cask.wrangler.steps.parser.JsParser;
import co.cask.wrangler.steps.parser.JsPath;
import co.cask.wrangler.steps.parser.ParseAvro;
import co.cask.wrangler.steps.parser.ParseAvroFile;
import co.cask.wrangler.steps.parser.ParseDate;
import co.cask.wrangler.steps.parser.ParseExcel;
import co.cask.wrangler.steps.parser.ParseLog;
import co.cask.wrangler.steps.parser.ParseProtobuf;
import co.cask.wrangler.steps.parser.ParseSimpleDate;
import co.cask.wrangler.steps.parser.XmlParser;
import co.cask.wrangler.steps.parser.XmlToJson;
import co.cask.wrangler.steps.transformation.CatalogLookup;
import co.cask.wrangler.steps.transformation.CharacterCut;
import co.cask.wrangler.steps.transformation.Decode;
import co.cask.wrangler.steps.transformation.Encode;
import co.cask.wrangler.steps.transformation.ExtractRegexGroups;
import co.cask.wrangler.steps.transformation.FillNullOrEmpty;
import co.cask.wrangler.steps.transformation.FindAndReplace;
import co.cask.wrangler.steps.transformation.GenerateUUID;
import co.cask.wrangler.steps.transformation.IndexSplit;
import co.cask.wrangler.steps.transformation.InvokeHttp;
import co.cask.wrangler.steps.transformation.LeftTrim;
import co.cask.wrangler.steps.transformation.Lower;
import co.cask.wrangler.steps.transformation.MaskNumber;
import co.cask.wrangler.steps.transformation.MaskShuffle;
import co.cask.wrangler.steps.transformation.MessageHash;
import co.cask.wrangler.steps.transformation.Quantization;
import co.cask.wrangler.steps.transformation.RightTrim;
import co.cask.wrangler.steps.transformation.Split;
import co.cask.wrangler.steps.transformation.SplitEmail;
import co.cask.wrangler.steps.transformation.SplitURL;
import co.cask.wrangler.steps.transformation.TableLookup;
import co.cask.wrangler.steps.transformation.TextDistanceMeasure;
import co.cask.wrangler.steps.transformation.TextMetricMeasure;
import co.cask.wrangler.steps.transformation.TitleCase;
import co.cask.wrangler.steps.transformation.Trim;
import co.cask.wrangler.steps.transformation.Upper;
import co.cask.wrangler.steps.transformation.UrlEncode;
import co.cask.wrangler.steps.transformation.XPathArrayElement;
import co.cask.wrangler.steps.transformation.XPathElement;
import co.cask.wrangler.steps.writer.WriteAsCSV;
import co.cask.wrangler.steps.writer.WriteAsJsonMap;
import co.cask.wrangler.steps.writer.WriteAsJsonObject;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import javax.annotation.Nullable;

/**
 * Parses the DSL into specification containing stepRegistry for wrangling.
 *
 * Following are some of the commands and format that {@link SimpleTextParser}
 * will handle.
 */
public class SimpleTextParser implements RecipeParser {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleTextParser.class);

  // directives for wrangling.
  private String[] directives;

  // Usage Registry
  private static final UsageRegistry usageRegistry = new UsageRegistry();

  // Specifies the context for directive parsing.
  private DirectiveContext context;

  public SimpleTextParser(String[] directives) {
    this.directives = directives;
    this.context = new NoOpDirectiveContext();
  }

  public SimpleTextParser(String directives) {
    this(directives.split("\n"));
  }

  public SimpleTextParser(List<String> directives) {
    this(directives.toArray(new String[directives.size()]));
  }

  /**
   * Parses the DSL to generate a sequence of stepRegistry to be executed by {@link RecipePipeline}.
   *
   * The transformation parsing here needs a better solution. It has many limitations and having different way would
   * allow us to provide much more advanced semantics for directives.
   *
   * @return List of stepRegistry to be executed.
   * @throws ParseException
   */
  @Override
  public List<Directive> parse() throws DirectiveParseException {
    List<Directive> directives = new ArrayList<>();

    // Split directive by EOL
    int lineno = 1;

    // Iterate through each directive and create necessary stepRegistry.
    for (String directive : this.directives) {
      directive = directive.trim();
      if (directive.isEmpty() || directive.startsWith("//") || directive.startsWith("#")) {
        continue;
      }

      StringTokenizer tokenizer = new StringTokenizer(directive, " ");
      String command = tokenizer.nextToken();

      // Check if a directive has been aliased and if it's aliased then retrieve root command it's mapped
      // to.
      String root = command;
      if (context.hasAlias(root)) {
        root = context.getAlias(command);
      }

      // Checks if the directive has been excluded from being used.
      if (!root.equals(command) && context.isExcluded(command)) {
        throw new DirectiveParseException(
          String.format("Aliased directive '%s' has been configured as restricted directive and is hence unavailable. " +
                          "Please contact your administrator", command)
        );
      }

      if (context.isExcluded(root)) {
        throw new DirectiveParseException(
          String.format("Directive '%s' has been configured as restricted directive and is hence unavailable. " +
                          "Please contact your administrator", command)
        );
      }

      switch (root) {

        // uppercase <col>
        case "uppercase": {
          String col = getNextToken(tokenizer, command, "col", lineno);
          directives.add(new Upper(lineno, directive, col));
        }
        break;

        // lowercase <col>
        case "lowercase": {
          String col = getNextToken(tokenizer, command, "col", lineno);
          directives.add(new Lower(lineno, directive, col));
        }
        break;

        // titlecase <col>
        case "titlecase": {
          String col = getNextToken(tokenizer, command, "col", lineno);
          directives.add(new TitleCase(lineno, directive, col));
        }
        break;

        // indexsplit <source> <start> <end> <destination>
        case "indexsplit": {
          String source = getNextToken(tokenizer, command, "source", lineno);
          String startStr = getNextToken(tokenizer, command, "start", lineno);
          String endStr = getNextToken(tokenizer, command, "end", lineno);
          int start = Integer.parseInt(startStr);
          int end = Integer.parseInt(endStr);
          String destination = getNextToken(tokenizer, command, "destination", lineno);
          directives.add(new IndexSplit(lineno, directive, source, start, end, destination));
        }
        break;

        // split <source-column-name> <delimiter> <new-column-1> <new-column-2>
        case "split": {
          String source = getNextToken(tokenizer, command, "source-column-name", lineno);
          String delimiter = getNextToken(tokenizer, command, "delimiter", lineno);
          String firstCol = getNextToken(tokenizer, command, "new-column-1", lineno);
          String secondCol = getNextToken(tokenizer, command, "new-column-2", lineno);
          directives.add(new Split(lineno, directive, source, delimiter, firstCol, secondCol));
        }
        break;

        // set-variable <variable> <expression>
        case "set-variable": {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String expression = getNextToken(tokenizer, "\n", command, "expression", lineno);
          directives.add(new SetTransientVariable(lineno, directive, column, expression));
        }
        break;

        // increment-variable <variable> <value> <expression>
        case "increment-variable": {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String value = getNextToken(tokenizer, command, "value", lineno);
          String expression = getNextToken(tokenizer, "\n", command, "expression", lineno);
          directives.add(new IncrementTransientVariable(lineno, directive, column, value, expression));
        }
        break;

        // mask-number <column> <pattern>
        case "mask-number": {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String mask = getNextToken(tokenizer, command, "pattern", lineno);
          directives.add(new MaskNumber(lineno, directive, column, mask));
        }
        break;

        // mask-shuffle <column>
        case "mask-shuffle": {
          String column = getNextToken(tokenizer, command, "column", lineno);
          directives.add(new MaskShuffle(lineno, directive, column));
        }
        break;

        // format-date <column> <destination>
        case "format-date": {
          String column = getNextToken(tokenizer, command, "column", 1);
          String format = getNextToken(tokenizer, "\n", command, "format", lineno);
          directives.add(new FormatDate(lineno, directive, column, format));
        }
        break;

        // format-unix-timestamp <column> <destination-format>
        case "format-unix-timestamp": {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String dstDatePattern = getNextToken(tokenizer, "\n", command, "destination-format", lineno);
          directives.add(new FormatDate(lineno, directive, column, dstDatePattern));
        }
        break;

        // quantize <source-column> <destination-column> <[range1:range2)=value>,[<range1:range2=value>]*
        case "quantize": {
          String column1 = getNextToken(tokenizer, command, "source-column", lineno);
          String column2 = getNextToken(tokenizer, command, "destination-column", lineno);
          String ranges = getNextToken(tokenizer, "\n", command, "destination-column", lineno);
          directives.add(new Quantization(lineno, directive, column1, column2, ranges));
        }
        break;

        // find-and-replace <column> <sed-script>
        case "find-and-replace" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String expression = getNextToken(tokenizer, "\n", command, "sed-script", lineno);
          directives.add(new FindAndReplace(lineno, directive, column, expression));
        }
        break;

        // parse-as-csv <column> <delimiter> [<header=true/false>]
        case "parse-as-csv" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String delimStr = getNextToken(tokenizer, command, "delimiter", lineno);
          char delimiter = delimStr.charAt(0);
          if (delimStr.startsWith("\\")) {
            String unescapedStr = StringEscapeUtils.unescapeJava(delimStr);
            if (unescapedStr == null) {
              throw new DirectiveParseException("Invalid delimiter for CSV Parser: " + delimStr);
            }
            delimiter = unescapedStr.charAt(0);
          }

          boolean hasHeader;
          String hasHeaderLinesOpt = getNextToken(tokenizer, "\n", command, "true|false", lineno, true);
          if (hasHeaderLinesOpt == null || hasHeaderLinesOpt.equalsIgnoreCase("false")) {
            hasHeader = false;
          } else {
            hasHeader = true;
          }
          CsvParser.Options opt = new CsvParser.Options(delimiter, true);
          directives.add(new CsvParser(lineno, directive, opt, column, hasHeader));
        }
        break;

        // parse-as-json <column> [depth]
        case "parse-as-json" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String depthOpt = getNextToken(tokenizer, "\n", command, "depth", lineno, true);
          int depth = Integer.MAX_VALUE;
          if (depthOpt != null && !depthOpt.isEmpty()) {
            try {
              depth = Integer.parseInt(depthOpt);
            } catch (NumberFormatException e) {
              throw new DirectiveParseException(
                String.format("Depth '%s' specified is not a valid number.", depthOpt)
              );
            }
          }
          directives.add(new JsParser(lineno, directive, column, depth));
        }
        break;

        // parse-as-avro <column> <schema-id> <json|binary> [version]
        case "parse-as-avro" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String schemaId = getNextToken(tokenizer, command, "schema-id", lineno);
          String type = getNextToken(tokenizer, command, "type", lineno);
          if (!"json".equalsIgnoreCase(type) && !"binary".equalsIgnoreCase(type)) {
           throw new DirectiveParseException(
             String.format("Parsing AVRO can be either of type 'json' or 'binary'")
           );
          }
          String versionOpt = getNextToken(tokenizer, "\n", command, "depth", lineno, true);
          int version = -1;
          if (versionOpt != null && !versionOpt.isEmpty()) {
            try {
              version = Integer.parseInt(versionOpt);
            } catch (NumberFormatException e) {
              throw new DirectiveParseException(
                String.format("Version '%s' specified is not a valid number.", versionOpt)
              );
            }
          }
          directives.add(new ParseAvro(lineno, directive, column, schemaId, type, version));
        }
        break;

        // parse-as-protobuf <column> <schema-id> <record-name> [version]
        case "parse-as-protobuf" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String schemaId = getNextToken(tokenizer, command, "schema-id", lineno);
          String recordName = getNextToken(tokenizer, command, "record-name", lineno);
          String versionOpt = getNextToken(tokenizer, "\n", command, "depth", lineno, true);
          int version = -1;
          if (versionOpt != null && !versionOpt.isEmpty()) {
            try {
              version = Integer.parseInt(versionOpt);
            } catch (NumberFormatException e) {
              throw new DirectiveParseException(
                String.format("Version '%s' specified is not a valid number.", versionOpt)
              );
            }
          }
          directives.add(new ParseProtobuf(lineno, directive, column, schemaId, recordName, version));
        }
        break;

        // json-path <source> <destination> <json-path>
        case "json-path" : {
          String src = getNextToken(tokenizer, command, "source", lineno);
          String dest = getNextToken(tokenizer, command, "dest", lineno);
          String path = getNextToken(tokenizer, "\n", command, "json-path", lineno);
          directives.add(new JsPath(lineno, directive, src, dest, path));
        }
        break;

        // set-charset <column> <charset>
        case "set-charset" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String charset = getNextToken(tokenizer, "\n", command, "charset", lineno, true);
          directives.add(new SetCharset(lineno, directive, column, charset));
        }
        break;

        // invoke-http <url> <column>[,<column>] <header>[,<header>]
        case "invoke-http" : {
          String url = getNextToken(tokenizer, command, "url", lineno);
          String columnsOpt = getNextToken(tokenizer, command, "columns", lineno);
          List<String> columns = new ArrayList<>();
          for (String column : columnsOpt.split(",")) {
            columns.add(column.trim());
          }
          String headers = getNextToken(tokenizer, "\n", command, "headers", lineno, true);
          directives.add(new InvokeHttp(lineno, directive, url, columns, headers));
        }
        break;


        // parse-as-fixed-length <column> <widths> [<padding>]
        case "parse-as-fixed-length" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String widthStr = getNextToken(tokenizer, command, "widths", lineno);
          String padding = getNextToken(tokenizer, "\n", column, "padding", lineno, true);
          if (padding == null || padding.isEmpty()) {
            padding = null; // Add space as padding.
          } else {
            padding = StringUtils.substringBetween(padding, "'", "'");
          }
          String[] widthsStr = widthStr.split(",");
          int[] widths = new int[widthsStr.length];
          int i = 0;
          for (String w : widthsStr) {
            try {
              widths[i] = Integer.parseInt(StringUtils.deleteWhitespace(w));
            } catch (NumberFormatException e) {
              throw new DirectiveParseException(
                String.format("Width specified '%s' at location %d is not a number.", w, i)
              );
            }
            ++i;
          }
          directives.add(new FixedLengthParser(lineno, directive, column, widths, padding));
        }
        break;

        // parse-xml-to-json <column> [<depth>]
        case "parse-xml-to-json" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String depthOpt = getNextToken(tokenizer, "\n", command, "depth", lineno, true);
          int depth = Integer.MAX_VALUE;
          try {
            if(depthOpt != null && !depthOpt.isEmpty()) {
              depth = Integer.parseInt(depthOpt);
            }
          } catch (NumberFormatException e) {
            throw new DirectiveParseException(e.getMessage());
          }
          directives.add(new XmlToJson(lineno, directive, column, depth));
        }
        break;

        // parse-as-xml <column>
        case "parse-as-xml" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          directives.add(new XmlParser(lineno, directive, column));
        }
        break;

        // parse-as-excel <column> <sheet number | sheet name>
        case "parse-as-excel" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String sheet = getNextToken(tokenizer, "\n", command, "sheet", lineno, true);
          directives.add(new ParseExcel(lineno, directive, column, sheet));
        }
        break;

        // xpath <column> <destination> <xpath>
        case "xpath" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String destination = getNextToken(tokenizer, command, "destination", lineno);
          String xpath = getNextToken(tokenizer, "\n", command, "xpath", lineno);
          directives.add(new XPathElement(lineno, directive, column, destination, xpath));
        }
        break;

        // xpath-array <column> <destination> <xpath>
        case "xpath-array" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String destination = getNextToken(tokenizer, command, "destination", lineno);
          String xpath = getNextToken(tokenizer, "\n", command, "xpath", lineno);
          directives.add(new XPathArrayElement(lineno, directive, column, destination, xpath));
        }
        break;

        // fill-null-or-empty <column> <fixed value>
        case "fill-null-or-empty" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String value = getNextToken(tokenizer, command, "fixed-value", lineno);
          if (value != null && value.isEmpty()) {
            throw new DirectiveParseException(
              "Fixed value cannot be a empty string"
            );
          }
          directives.add(new FillNullOrEmpty(lineno, directive, column, value));
        }
        break;

        // cut-character <source> <destination> <range|indexes>
        case "cut-character" : {
          String source = getNextToken(tokenizer, command, "source", lineno);
          String destination = getNextToken(tokenizer, command, "destination", lineno);
          String range = getNextToken(tokenizer, command, "range", lineno);
          directives.add(new CharacterCut(lineno, directive, source, destination, range));
        }
        break;

        // generate-uuid <column>
        case "generate-uuid" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          directives.add(new GenerateUUID(lineno, directive, column));
        }
        break;

        // url-encode <column>
        case "url-encode" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          directives.add(new UrlEncode(lineno, directive, column));
        }
        break;

        // url-decode <column>
        case "url-decode" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          directives.add(new UrlEncode(lineno, directive, column));
        }
        break;

        // parse-as-log <column> <format>
        case "parse-as-log" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String format = getNextToken(tokenizer, "\n", command, "format", lineno);
          directives.add(new ParseLog(lineno, directive, column, format));
        }
        break;

        // parse-as-date <column> [<timezone>]
        case "parse-as-date" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String timezone = getNextToken(tokenizer, "\n", command, "timezone", lineno, true);
          directives.add(new ParseDate(lineno, directive, column, timezone));
        }
        break;

        // parse-as-simple-date <column> <pattern>
        case "parse-as-simple-date" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String pattern = getNextToken(tokenizer, "\n", command, "format", lineno);
          directives.add(new ParseSimpleDate(lineno, directive, column, pattern));
        }
        break;

        // diff-date <column1> <column2> <destColumn>
        case "diff-date" : {
          String column1 = getNextToken(tokenizer, command, "column1", lineno);
          String column2 = getNextToken(tokenizer, command, "column2", lineno);
          String destColumn = getNextToken(tokenizer, "\n", command, "destColumn", lineno);
          directives.add(new DiffDate(lineno, directive, column1, column2, destColumn));
        }
        break;

        // parse-as-hl7 <column> [<depth>]
        case "parse-as-hl7" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String depthOpt = getNextToken(tokenizer, "\n", command, "depth", lineno, true);
          int depth = Integer.MAX_VALUE;
          try {
            if (depthOpt != null && !depthOpt.isEmpty()) {
              depth = Integer.parseInt(depthOpt);
            }
          } catch (NumberFormatException e) {
            throw new DirectiveParseException(e.getMessage());
          }
          directives.add(new HL7Parser(lineno, directive, column, depth));
        }
        break;

        // split-email <column>
        case "split-email" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          directives.add(new SplitEmail(lineno, directive, column));
        }
        break;

        // hash <column> <algorithm> [encode]
        case "hash" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String algorithm = getNextToken(tokenizer, command, "algorithm", lineno);
          String encodeOpt = getNextToken(tokenizer, "\n", command, "encode", lineno, true);
          if (!MessageHash.isValid(algorithm)) {
            throw new DirectiveParseException(
              String.format("Algorithm '%s' specified in directive '%s' at line %d is not supported", algorithm,
                            command, lineno)
            );
          }

          boolean encode = true;
          if (encodeOpt.equalsIgnoreCase("false")) {
            encode = false;
          }

          try {
            MessageDigest digest = MessageDigest.getInstance(algorithm);
            directives.add(new MessageHash(lineno, directive, column, digest, encode));
          } catch (NoSuchAlgorithmException e) {
            throw new DirectiveParseException(
              String.format("Unable to find algorithm specified '%s' in directive '%s' at line %d.",
                            algorithm, command, lineno)
            );
          }
        }
        break;

        // write-as-json-map <column>
        case "write-as-json-map" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          directives.add(new WriteAsJsonMap(lineno, directive, column));
        }
        break;

        // write-as-json-object <dest-column> [<src-column>[,<src-column>]
        case "write-as-json-object" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String columnsStr = getNextToken(tokenizer, "\n", command, "columns", lineno);
          if (columnsStr != null) {
            List<String> columns = new ArrayList<>();
            for (String col : columnsStr.split(",")) {
              columns.add(col.trim());
            }
            directives.add(new WriteAsJsonObject(lineno, directive, column, columns));
          } else {
            throw new DirectiveParseException(
              String.format("")
            );
          }
        }
        break;

        // write-as-csv <column>
        case "write-as-csv" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          directives.add(new WriteAsCSV(lineno, directive, column));
        }
        break;

        // parse-as-avro-file <column>
        case "parse-as-avro-file": {
          String column = getNextToken(tokenizer, command, "column", lineno);
          directives.add(new ParseAvroFile(lineno, directive, column));
        }
        break;

        // text-distance <method> <column1> <column2> <destination>
        case "text-distance" : {
          String method = getNextToken(tokenizer, command, "method", lineno);
          String column1 = getNextToken(tokenizer, command, "column1", lineno);
          String column2 = getNextToken(tokenizer, command, "column2", lineno);
          String destination = getNextToken(tokenizer, command, "destination", lineno);
          directives.add(new TextDistanceMeasure(lineno, directive, method, column1, column2, destination));
        }
        break;

        // text-metric <method> <column1> <column2> <destination>
        case "text-metric" : {
          String method = getNextToken(tokenizer, command, "method", lineno);
          String column1 = getNextToken(tokenizer, command, "column1", lineno);
          String column2 = getNextToken(tokenizer, command, "column2", lineno);
          String destination = getNextToken(tokenizer, command, "destination", lineno);
          directives.add(new TextMetricMeasure(lineno, directive, method, column1, column2, destination));
        }
        break;

        // catalog-lookup ICD-9|ICD-10 <column>
        case "catalog-lookup" : {
          String type = getNextToken(tokenizer, command, "type", lineno);
          String column = getNextToken(tokenizer, command, "column", lineno);
          if (!type.equalsIgnoreCase("ICD-9") && !type.equalsIgnoreCase("ICD-10-2016") &&
              !type.equalsIgnoreCase("ICD-10-2017")) {
            throw new IllegalArgumentException("Invalid ICD type - should be 9 (ICD-9) or 10 (ICD-10-2016 " +
                                                 "or ICD-10-2017).");
          } else {
            ICDCatalog catalog = new ICDCatalog(type.toLowerCase());
            if (!catalog.configure()) {
              throw new DirectiveParseException(
                String.format("Failed to configure ICD StaticCatalog. Check with your administrator")
              );
            }
            directives.add(new CatalogLookup(lineno, directive, catalog, column));
          }
        }
        break;

        // table-lookup <column> <table>
        case "table-lookup" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String table = getNextToken(tokenizer, command, "table", lineno);
          directives.add(new TableLookup(lineno, directive, column, table));
        }
        break;

        // stemming <column>
        case "stemming" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          directives.add(new Stemming(lineno, directive, column));
        }
        break;

        // extract-regex-groups <column> <regex>
        case "extract-regex-groups" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          String regex = getNextToken(tokenizer, command, "regex", lineno);
          directives.add(new ExtractRegexGroups(lineno, directive, column, regex));
        }
        break;

        // split-url <column>
        case "split-url" : {
          String column = getNextToken(tokenizer, command, "column", lineno);
          directives.add(new SplitURL(lineno, directive, column));
        }
        break;

        // encode <base32|base64|hex> <column>
        case "encode" : {
          String type = getNextToken(tokenizer, command, "type", lineno);
          String column = getNextToken(tokenizer, command, "column", lineno);
          type = type.toUpperCase();
          if (!type.equals("BASE64") && !type.equals("BASE32") && !type.equals("HEX")) {
            throw new DirectiveParseException(
              String.format("Type of encoding specified '%s' is not supported. Supports base64, base32 & hex.",
                            type)
            );
          }
          directives.add(new Encode(lineno, directive, Encode.Type.valueOf(type), column));
        }
        break;

        // decode <base32|base64|hex> <column>
        case "decode" : {
          String type = getNextToken(tokenizer, command, "type", lineno);
          String column = getNextToken(tokenizer, command, "column", lineno);
          type = type.toUpperCase();
          if (!type.equals("BASE64") && !type.equals("BASE32") && !type.equals("HEX")) {
            throw new DirectiveParseException(
              String.format("Type of decoding specified '%s' is not supported. Supports base64, base32 & hex.",
                            type)
            );
          }
          directives.add(new Decode(lineno, directive, Decode.Type.valueOf(type), column));
        }
        break;

        //trim <column>
        case "trim": {
          String col = getNextToken(tokenizer, command, "col", lineno);
          directives.add(new Trim(lineno, directive, col));
        }
        break;

        //ltrim <column>
        case "ltrim": {
          String col = getNextToken(tokenizer, command, "col", lineno);
          directives.add(new LeftTrim(lineno, directive, col));
        }
        break;

        //rtrim <column>
        case "rtrim": {
          String col = getNextToken(tokenizer, command, "col", lineno);
          directives.add(new RightTrim(lineno, directive, col));
        }
        break;

        default:
          throw new DirectiveParseException(
            String.format("Unknown directive '%s' found in the directive at line %d", command, lineno)
          );
      }
      lineno++;
    }
    return directives;
  }

  // If there are more tokens, then it proceeds with parsing, else throws exception.
  public static String getNextToken(StringTokenizer tokenizer, String directive,
                          String field, int lineno) throws DirectiveParseException {
    return getNextToken(tokenizer, null, directive, field, lineno, false);
  }

  public static String getNextToken(StringTokenizer tokenizer, String delimiter,
                              String directive, String field, int lineno) throws DirectiveParseException {
    return getNextToken(tokenizer, delimiter, directive, field, lineno, false);
  }

  public static String getNextToken(StringTokenizer tokenizer, String delimiter,
                          String directive, String field, int lineno, boolean optional)
    throws DirectiveParseException {
    String value = null;
    if (tokenizer.hasMoreTokens()) {
      if (delimiter == null) {
        value = tokenizer.nextToken().trim();
      } else {
        value = tokenizer.nextToken(delimiter).trim();
      }
    } else {
      if (!optional) {
        String usage = usageRegistry.getUsage(directive);
        throw new DirectiveParseException(
          String.format("Missing field '%s' at line number %d for directive <%s> (usage: %s)",
                        field, lineno, directive, usage)
        );
      }
    }
    return value;
  }

  /**
   * Initialises the directive with a {@link DirectiveContext}.
   *
   * @param context
   */
  @Nullable
  @Override
  public void initialize(DirectiveContext context) {
    if (context == null) {
      this.context = new NoOpDirectiveContext();
    } else {
      this.context = context;
    }
  }
}
