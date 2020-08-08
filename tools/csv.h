#pragma once

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <functional>

#include "helpers.h"

using CSVRow = std::vector<std::string>;

/** Reads the CSV file line by line.

    This class provides a very basic, but reasonably robust CSV reader. It can even deal with situations such as unescaped columns spanning multiple lines (by calling append in row).

    TODO This needs much more work to be actually useful, but is a start...
*/
class BaseCSVReader {
public:

    BaseCSVReader(char quote = '"', char separator = ','):
        quote_(quote),
        separator_(separator) {
    }

protected:
    /** This method gets called whenever a CSV row has been parsed with the columns stored in the row argument.

        This method must be override in subclasses since its implementation actually provides the implementation. 
    */
    virtual void row(std::vector<std::string> & row)  = 0;

    virtual void error(std::ios_base::failure const & e) {
        std::cerr << "line " << lineNum_  << ": " << e.what() << std::endl;
    }

    /** Parses the given file.

        If the file cannot be opened, throws the ios_base::failure exception, otherwise parses the file and when parsing of a line is finished, calls the row() method repeatedly until the end of file is reached.
    */
    size_t parse(std::string const & filename, bool headers) {
        f_ = std::ifstream(filename, std::ios::in);
        lineNum_ = 1;
        numRows_ = 0;
        //        f_.open(filename, std::ios::in);
        if (! f_.good())
            throw std::ios_base::failure(STR("Unable to openfile " << filename));
        while (! eof()) {
            try {
                append();
                if (!row_.empty()) {
                    if (headers) {
                        headers = false;
                    } else {
                        row(row_);
                        ++numRows_;
                        if (lineNum_ % 1000 == 0) {
                            std::cout << " : " << (lineNum_/1000) << "k\r" << std::flush;
                        } 
                    }
                    row_.clear();
                }
            } catch(std::ios_base::failure const & e) {
                error(e);
            }
        }
        f_.close();
        return numRows_;
    }

    /** Returns true if the end of input file has been reached.
     */
    bool eof() const {
        return f_.eof();
    }

    /** Reads next line of the CSV appending it to the existing row vector.

        The parser is fairly simple for now, accepts non-quoted and quoted columns. Quoted columns may span multiple lines and their line endings can be escaped, in which case they will appear as single line.

        TODO make this function more robust.
    */
    void append() {
        std::string line = readLine();;
        std::string startLine = line;
        size_t i = 0;
        bool isFirst = true;
        while (i < line.size()) {
            std::string col;
            // first check quoted string 
            if (line[i] == quote_) {
                size_t quoteStart = lineNum_;
                ++i;
                while (line[i] != quote_) {
                    if (i == line.size() - 1 && eof())
                        throw std::ios_base::failure(STR("Unterminated quote, starting at line " + quoteStart));
                    // check for escaped characters
                    if (line[i] == '\\') {
                        ++i;
                        // if the new line is escaped, we already have the escaped character
                        if (i == line.size()) {
                            while (i == line.size() && ! eof()) {
                                col += '\n';
                                line = readLine();
                                i = 0;
                            }
                            // otherwise just add the escaped character
                        } else {
                            col += line[i++];
                        }
                        // normal character, just append
                    } else {
                        col += line[i++];
                    }
                    while (i == line.size() && ! eof()) {
                        line = readLine();
                        i = 0;
                        col += "\n";
                    }
                }
                ++i; // past the ending quote
                // if immediately followed by non-whitespace and not separator, add this to the column as well
                // FIXME We need this because shabbir's downloader did not output CSVs nicely, but this is not part of CSV standard
                // if (line[i] != ' ' && line[i] != '\t' && line[i] != separator_ && line[i] != 0) {
                //    col = quote_ + col + quote_;
                //    while (i < line.size() && line[i] != separator_)
                //        col = col + line[i++];
                // }
                if (line[i] == separator_)
                    ++i;
            } else {
                while (i < line.size()) {
                    // prefixed strings - i.e. some stuff followed by quoted string, we keep the quotes
                    if (line[i] == quote_) {
                        col += line[i++];
                        std::string xl = line;
                        size_t quoteStart = lineNum_;
                        while(line[i] != quote_) {
                            if (eof())
                                throw std::ios_base::failure(STR("Unterminated quote, starting at line " << quoteStart << line));
                            if (line[i] == '\\') {
                                ++i;
                                while (i == line.size() && ! eof()) {
                                    line = readLine();
                                    i = 0;
                                }
                            }
                            col += line[i++];
                            while (i == line.size() && ! eof()) {
                                line = readLine();
                                i = 0;
                                col += "\n";
                            }
                        }
                        col += line[i++]; // the ending quote
                    }
                    if (line[i] == separator_) {
                        ++i;
                        break;
                    }
                    col += line[i++];
                }
            }
            // now we have the column in col
            addColumn(col, isFirst);
            isFirst = false;
        }
        if (line[line.size() - 1] == separator_)
            addColumn("", false);
    }

    /** Returns the number of rows properly read.
     */
    size_t numRows() const {
        return numRows_;
    }

    /** Returns the number of lines read. A row may span multiple lines.
     */
    size_t numLines() const {
        return lineNum_;
    }

private:

    /** Reads next line from the input file.
     */
    std::string readLine() {
        std::string line;
        if (std::getline(f_, line))
            ++lineNum_;
        return line;
    }

    /** Adds next column to the currently processed row.

        If append is true and the row is not empty, then no new column will be created, but the value will be appended to the last existing column.
    */
    void addColumn(std::string const & col, bool append) {
        if (append && ! row_.empty()) {
            row_.back() = row_.back() + col;
        } else {
            row_.push_back(col);
        }
    }
    
    std::ifstream f_;
    std::vector<std::string> row_;
    size_t lineNum_;
    size_t numRows_;

    char quote_;
    char separator_;
}; // BaseCSVReader

class CSVReader : public BaseCSVReader {
public:

    using RowHandler = std::function<void(std::vector<std::string> &)>;

    static size_t Parse(std::string const & filename, RowHandler handler, bool headers = true, char quote = '"', char separator = ',') {
        CSVReader r{quote, separator, handler};
        return r.parse(filename, headers);
    }

protected:

    CSVReader(char quote, char separator, RowHandler handler):
        BaseCSVReader{quote, separator},
        rowHandler_{handler} {
    }
    
    void row(std::vector<std::string> & row) override {
        rowHandler_(row);
    }
    
private:
    
    RowHandler rowHandler_;

    
}; 
