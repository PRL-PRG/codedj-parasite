#pragma once
#include <cassert>
#include <ctime>
#include <algorithm>

#include <sstream>
#include <string>
#include <iostream>
#include <vector>

#define STR(...) static_cast<std::stringstream &&>(std::stringstream() << __VA_ARGS__).str()

inline std::string EscapeQuotes(std::string const & from) {
    std::string result = "\"";
    for (char c : from) {
        switch (c) {
        case '\'':
        case '"':
        case '\\':
            result = result + '\\' + c;
            break;
        default:
            result += c;
        }
    }
    result = result + "\"";
    return result;
}

inline std::vector<std::string> Split(std::string const & what, char delimiter) {
    std::vector<std::string> result;
    size_t start = 0;
    for (size_t i = 0, e = what.size(); i != e; ++i) {
        if (what[i] == delimiter) {
            result.push_back(what.substr(start, i - start));
            start = i + 1;
        }
    }
    result.push_back(what.substr(start, what.size() - start));
    return result;
}

inline std::vector<std::string> Split(std::string const & what, char delimiter, size_t limit) {
    std::vector<std::string> result;

    if (limit < 2) {
        result.push_back(what);
        return  result;
    }

    size_t start = 0;
    for (size_t i = 0, e = what.size(); i != e; ++i) {
        if (what[i] == delimiter) {
            result.push_back(what.substr(start, i - start));
            start = i + 1;

            if (result.size() == limit - 1) {
                break;
            }
        }
    }
    result.push_back(what.substr(start, what.size() - start));
    return result;
}

inline bool StartsWith(std::string const & value, std::string const & prefix) {
    return value.find(prefix) == 0;
}
    
inline bool EndsWith(std::string const & value, std::string const & ending) {
    if (ending.size() > value.size())
        return false;
    return std::equal(ending.rbegin(), ending.rend(), value.rbegin());
}

inline std::string Strip(std::string s) {
    s.erase(0, s.find_first_not_of("\t\n\v\f\r "));
    s.erase(s.find_last_not_of("\t\n\v\f\r ") + 1);
    return s;
}

inline std::string ToLower(std::string const & from) {
    std::string result = from;
    std::transform(result.begin(), result.end(), result.begin(),[](unsigned char c){ return std::tolower(c); });
    return result;
}

inline size_t TimeNow() {
    std::time_t result = std::time(nullptr);
    return result;
}

/** Returns the time in the format used by the HTTP standard. 
 */
inline std::string TimeRFC1123(size_t epoch) {
    std::time_t t = epoch;
    struct tm * tm = gmtime(&t);
    char buffer[128];
    strftime(buffer, 128, "%a, %d %b %Y %H:%M:%S GMT", tm);
    return std::string{buffer};
}

inline unsigned CharToHex(char what) {
    if (what >= '0' && what <= '9')
        return what - '0';
    return 0;
          
}



inline std::string PrettyDHMS(size_t time) {
    size_t s = time % 60;
    time = time / 60;
    size_t m = time % 60;
    time = time / 60;
    size_t h = time % 24;
    time = time / 24;
    bool write = false;
    std::stringstream result;
    if (time > 0) {
        result << time << "d ";
        write = true;
    }
    if (write || h > 0) {
        result << h << "h ";
        write = true;
    }
    if (write || m > 0) {
        result << m << "m ";
        write = true;
    }
    result << s << "s";
    return result.str();
}

inline std::string PrettyPct(size_t x, size_t max) {
    if (max == 0)
        return "--%";
    size_t pct = x * 100 / max;
    return STR(pct << "%");
}

inline std::string PrettyPctBar(size_t x, size_t max, size_t width) {
    std::stringstream s;
    if (max == 0) {
        s << " [";
        for (size_t i = 0; i < width; ++i)
            s << " ";
        s << "] --%";
    } else {
        size_t pct = x * 100 / max;
        size_t bar = pct * width / (100 * width);
        s << " [";
        for (size_t i = 0; i < width; ++i)
            s << ((i < bar) ? "#" : " ");
        s << "] ";
        if (pct < 10)
            s << " ";
        s << pct << "%";
    }
    return s.str();
}
