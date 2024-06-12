#pragma once

#include "query/executor/expr/expr.h"
#include <cstring>

class ExprLike : public Expr {
public:
    ExprLike(std::unique_ptr<Expr> child, std::string&& _pattern) :
        child   (std::move(child)),
        pattern (std::move(_pattern)),
        res     ((int64_t)0) { }

    const Value& eval() override {
        auto& child_res = child->eval();
        auto pattern_res = pattern.data();
        auto child_str = child_res.value.as_str;
        auto child_str_size = strlen(child_str);
        size_t i = 0;
        size_t checkpoint_i = 0;
        size_t j = 0;
        size_t checkpoint_j = 0;
        while(i < child_str_size && j < pattern.size()) {
            if(pattern_res[j] == '%') {
                j++;
                if(j == pattern.size()) {
                    res.value.as_int = static_cast<int64_t>(1);
                    return res;
                }
                while(i < child_str_size && child_str[i] != pattern_res[j]) {
                    i++;
                    checkpoint_i = i;
                    checkpoint_j = j-1;
                }
            } else if(pattern_res[j] == '_') {
                i++;
                j++;
            } else {
                if(child_str[i] != pattern_res[j]) {
                    if(checkpoint_i != 0) {
                        i = checkpoint_i;
                        j = checkpoint_j-1;
                        checkpoint_i = 0;
                        checkpoint_j = 0;
                    } else {
                        res.value.as_int = static_cast<int64_t>(0);
                        return res;
                    }
                }
                i++;
                j++;
            }
        }
        if(i == child_str_size && (j == pattern.size() || (j + 1 == pattern.size() && pattern_res[j] == '%'))) {
            res.value.as_int = static_cast<int64_t>(1);
            return res;
        }
        res.value.as_int = static_cast<int64_t>(0);
        return res;
    }

    std::ostream& print_to_ostream(std::ostream& os) const override {
        os << *child << " LIKE \"" << pattern << '"';
        return os;
    }

private:
    std::unique_ptr<Expr> child;

    std::string pattern;

    Value res;
};