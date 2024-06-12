#pragma once

#include "query/parser/logical_plan/expr/expr_plan.h"
#include "query/parser/logical_plan/expr/expr_plan_column.h"
#include "query/parser/logical_plan/logical_plan.h"
#include "relational_model/value.h"


class ExprPlanBetween : public ExprPlan {
public:
    std::unique_ptr<ExprPlanColumn> column;
    std::unique_ptr<Value> low;
    std::unique_ptr<Value> high;

    ExprPlanBetween(std::unique_ptr<ExprPlanColumn> column, std::unique_ptr<Value> low, std::unique_ptr<Value> high) :
        column(std::move(column)),
        low(std::move(low)),
        high(std::move(high)) { }

    std::unique_ptr<ExprPlan> clone() const override {
        return std::make_unique<ExprPlanBetween>(
            std::make_unique<ExprPlanColumn>(*column),
            std::make_unique<Value>(*low),
            std::make_unique<Value>(*high)
        );
    }

    void accept_visitor(ExprPlanVisitor& visitor) override {
        visitor.visit(*this);
    }

    DataType get_datatype() const override {
        return column->column.datatype;
    }

    std::set<Column> get_columns() const override {
        return { column->column };
    }

    std::ostream& print_to_ostream(std::ostream& os) const override {
        os << column->column.alias << '.' << column->column.column;
        os << " BETWEEN \"";
        os << *low;
        os << " AND ";
        os << *high;
        os << "\"";
        return os;
    }
};