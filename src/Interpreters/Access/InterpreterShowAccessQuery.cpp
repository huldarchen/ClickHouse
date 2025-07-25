#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/Access/InterpreterShowAccessQuery.h>

#include <Interpreters/Context.h>
#include <Interpreters/Access/InterpreterShowCreateAccessEntityQuery.h>
#include <Interpreters/Access/InterpreterShowGrantsQuery.h>
#include <Interpreters/formatWithPossiblyHidingSecrets.h>
#include <Columns/ColumnString.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <DataTypes/DataTypeString.h>
#include <Access/Common/AccessFlags.h>
#include <Access/AccessControl.h>
#include <base/range.h>
#include <base/sort.h>
#include <base/insertAtEnd.h>


namespace DB
{

BlockIO InterpreterShowAccessQuery::execute()
{
    BlockIO res;
    res.pipeline = executeImpl();
    return res;
}


QueryPipeline InterpreterShowAccessQuery::executeImpl() const
{
    /// Build a create query.
    ASTs queries = getCreateAndGrantQueries();

    /// Build the result column.
    MutableColumnPtr column = ColumnString::create();
    for (const auto & query : queries)
        column->insert(format({getContext(), *query}));

    String desc = "ACCESS";
    return QueryPipeline(std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(Block{{std::move(column), std::make_shared<DataTypeString>(), desc}})));
}


std::vector<AccessEntityPtr> InterpreterShowAccessQuery::getEntities() const
{
    const auto & access_control = getContext()->getAccessControl();
    getContext()->checkAccess(AccessType::SHOW_ACCESS);

    std::vector<AccessEntityPtr> entities;
    for (auto type : collections::range(AccessEntityType::MAX))
    {
        auto ids = access_control.findAll(type);
        for (const auto & id : ids)
        {
            if (auto entity = access_control.tryRead(id))
                entities.push_back(entity);
        }
    }

    ::sort(entities.begin(), entities.end(), IAccessEntity::LessByTypeAndName{});
    return entities;
}


ASTs InterpreterShowAccessQuery::getCreateAndGrantQueries() const
{
    auto entities = getEntities();
    const auto & access_control = getContext()->getAccessControl();

    ASTs create_queries;
    ASTs grant_queries;
    for (const auto & entity : entities)
    {
        create_queries.push_back(InterpreterShowCreateAccessEntityQuery::getCreateQuery(*entity, access_control));
        if (entity->isTypeOf(AccessEntityType::USER) || entity->isTypeOf(AccessEntityType::ROLE))
            insertAtEnd(grant_queries, InterpreterShowGrantsQuery::getGrantQueries(*entity, access_control));
    }

    ASTs result = std::move(create_queries);
    insertAtEnd(result, std::move(grant_queries));
    return result;
}

void registerInterpreterShowAccessQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterShowAccessQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterShowAccessQuery", create_fn);
}

}
