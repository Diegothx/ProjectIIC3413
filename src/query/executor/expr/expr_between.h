#pragma once

#include "query/executor/expr/expr.h"
#include <cstring>
class ExprBetween : public Expr {
public:
    std::unique_ptr<Value> low;
    std::unique_ptr<Value> high;

    ExprBetween(std::unique_ptr<Expr> child, std::unique_ptr<Value> low, std::unique_ptr<Value> high) :
        low   (std::move(low)),
        high (std::move(high)),
        child   (std::move(child)),
        res     ((int64_t)0) { }

        const Value& eval() override {
        auto child_res = child->eval();
        Value& low_res = *low;
        Value& high_res = *high;
        res.value.as_int = static_cast<int64_t>(low_res <= child_res && child_res <= high_res);

        return res;
    }

    std::ostream& print_to_ostream(std::ostream& os) const override {
        os << *child;
        os << " BETWEEN \"";
        os << *low;
        os << " AND ";
        os << *high;
        os << '"';
        return os;
    }

private:
    std::unique_ptr<Expr> child;

    std::string pattern;

    Value res;
};