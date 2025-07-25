#pragma once

#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include "config.h"

#if USE_EMBEDDED_COMPILER
#    include <llvm/IR/IRBuilder.h>
#    include <DataTypes/Native.h>
#endif

namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/** Not an aggregate function, but an adapter of aggregate functions,
  * which any aggregate function `agg(x)` makes an aggregate function of the form `aggIf(x, cond)`.
  * The adapted aggregate function takes two arguments - a value and a condition,
  * and calculates the nested aggregate function for the values when the condition is satisfied.
  * For example, avgIf(x, cond) calculates the average x if `cond`.
  */
class AggregateFunctionIf final : public IAggregateFunctionHelper<AggregateFunctionIf>
{
private:
    AggregateFunctionPtr nested_func;
    size_t num_arguments;
    /// We accept Nullable(Nothing) as condition, but callees always expect UInt8 so we need to avoid calling them
    bool only_null_condition = false;

public:
    AggregateFunctionIf(AggregateFunctionPtr nested, const DataTypes & types, const Array & params_)
        : IAggregateFunctionHelper<AggregateFunctionIf>(types, params_, nested->getResultType())
        , nested_func(nested), num_arguments(types.size())
    {
        if (num_arguments == 0)
            throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "Aggregate function {} require at least one argument", getName());

        only_null_condition = types.back()->onlyNull();

        if (!isUInt8(types.back()) && !only_null_condition)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Last argument for aggregate function {} must be UInt8", getName());
    }

    String getName() const override
    {
        return nested_func->getName() + "If";
    }

    const IAggregateFunction & getBaseAggregateFunctionWithSameStateRepresentation() const override
    {
        return nested_func->getBaseAggregateFunctionWithSameStateRepresentation();
    }

    DataTypePtr getNormalizedStateType() const override
    {
        return nested_func->getNormalizedStateType();
    }

    bool isVersioned() const override
    {
        return nested_func->isVersioned();
    }

    size_t getVersionFromRevision(size_t revision) const override
    {
        return nested_func->getVersionFromRevision(revision);
    }

    size_t getDefaultVersion() const override
    {
        return nested_func->getDefaultVersion();
    }

    void create(AggregateDataPtr __restrict place) const override
    {
        nested_func->create(place);
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        nested_func->destroy(place);
    }

    void destroyUpToState(AggregateDataPtr __restrict place) const noexcept override
    {
        nested_func->destroyUpToState(place);
    }

    bool hasTrivialDestructor() const override
    {
        return nested_func->hasTrivialDestructor();
    }

    size_t sizeOfData() const override
    {
        return nested_func->sizeOfData();
    }

    size_t alignOfData() const override
    {
        return nested_func->alignOfData();
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        if (only_null_condition)
            return;
        if (assert_cast<const ColumnUInt8 &>(*columns[num_arguments - 1]).getData()[row_num])
            nested_func->add(place, columns, row_num, arena);
    }

    void addManyDefaults(AggregateDataPtr __restrict place, const IColumn ** columns, size_t length, Arena * arena) const override
    {
        if (only_null_condition)
            return;
        if (assert_cast<const ColumnUInt8 &>(*columns[num_arguments - 1]).getData()[0])
            nested_func->addManyDefaults(place, columns, length, arena);
    }

    void addBatch(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * __restrict places,
        size_t place_offset,
        const IColumn ** columns,
        Arena * arena,
        ssize_t) const override
    {
        if (only_null_condition)
            return;
        nested_func->addBatch(row_begin, row_end, places, place_offset, columns, arena, num_arguments - 1);
    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        Arena * arena,
        ssize_t) const override
    {
        if (only_null_condition)
            return;
        nested_func->addBatchSinglePlace(row_begin, row_end, place, columns, arena, num_arguments - 1);
    }

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena * arena,
        ssize_t) const override
    {
        if (only_null_condition)
            return;
        nested_func->addBatchSinglePlaceNotNull(row_begin, row_end, place, columns, null_map, arena, num_arguments - 1);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        nested_func->merge(place, rhs, arena);
    }

    bool isAbleToParallelizeMerge() const override { return nested_func->isAbleToParallelizeMerge(); }
    bool canOptimizeEqualKeysRanges() const override { return nested_func->canOptimizeEqualKeysRanges(); }

    void parallelizeMergePrepare(AggregateDataPtrs & places, ThreadPool & thread_pool, std::atomic<bool> & is_cancelled) const override
    {
        nested_func->parallelizeMergePrepare(places, thread_pool, is_cancelled);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, ThreadPool & thread_pool, std::atomic<bool> & is_cancelled, Arena * arena) const override
    {
        nested_func->merge(place, rhs, thread_pool, is_cancelled, arena);
    }

    void mergeBatch(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * places,
        size_t place_offset,
        const AggregateDataPtr * rhs,
        ThreadPool & thread_pool,
        std::atomic<bool> & is_cancelled,
        Arena * arena) const override
    {
        nested_func->mergeBatch(row_begin, row_end, places, place_offset, rhs, thread_pool, is_cancelled, arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const override
    {
        nested_func->serialize(place, buf, version);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena * arena) const override
    {
        nested_func->deserialize(place, buf, version, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        nested_func->insertResultInto(place, to, arena);
    }

    void insertMergeResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        nested_func->insertMergeResultInto(place, to, arena);
    }

    bool allocatesMemoryInArena() const override
    {
        return nested_func->allocatesMemoryInArena();
    }

    bool isState() const override
    {
        return nested_func->isState();
    }

    AggregateFunctionPtr getOwnNullAdapter(
        const AggregateFunctionPtr & nested_function, const DataTypes & arguments,
        const Array & params, const AggregateFunctionProperties & properties) const override;

    AggregateFunctionPtr getNestedFunction() const override { return nested_func; }

    std::unordered_set<size_t> getArgumentsThatCanBeOnlyNull() const override
    {
        return {num_arguments - 1};
    }

#if USE_EMBEDDED_COMPILER

    bool isCompilable() const override
    {
        return canBeNativeType(*this->argument_types.back()) && nested_func->isCompilable();
    }

    void compileCreate(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override
    {
        nested_func->compileCreate(builder, aggregate_data_ptr);
    }

    void compileAdd(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, const ValuesWithType & arguments) const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        const auto & predicate_type = arguments.back().type;
        auto * predicate_value = arguments.back().value;

        auto * head = b.GetInsertBlock();

        auto * join_block = llvm::BasicBlock::Create(head->getContext(), "join_block", head->getParent());
        auto * if_true = llvm::BasicBlock::Create(head->getContext(), "if_true", head->getParent());
        auto * if_false = llvm::BasicBlock::Create(head->getContext(), "if_false", head->getParent());

        auto * is_predicate_true = nativeBoolCast(b, predicate_type, predicate_value);

        b.CreateCondBr(is_predicate_true, if_true, if_false);

        b.SetInsertPoint(if_true);

        ValuesWithType arguments_without_predicate = arguments;
        arguments_without_predicate.pop_back();
        nested_func->compileAdd(builder, aggregate_data_ptr, arguments_without_predicate);

        b.CreateBr(join_block);

        b.SetInsertPoint(if_false);
        b.CreateBr(join_block);

        b.SetInsertPoint(join_block);
    }

    void compileMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr) const override
    {
        nested_func->compileMerge(builder, aggregate_data_dst_ptr, aggregate_data_src_ptr);
    }

    llvm::Value * compileGetResult(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override
    {
        return nested_func->compileGetResult(builder, aggregate_data_ptr);
    }

#endif


};

}
