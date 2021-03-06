#include <string>
#include <deque>
#include <limits>
#include <hiredis/hiredis.h>
#include "dbconnector.h"
#include "table.h"
#include "selectable.h"
#include "redisselect.h"
#include "redisapi.h"
#include "consumerstatetable.h"

namespace swss {

ConsumerStateTable::ConsumerStateTable(DBConnector *db, std::string tableName, int popBatchSize)
    : RedisTransactioner(db)
    , TableName_KeySet(tableName)
    , POP_BATCH_SIZE(popBatchSize)
{
    for (;;)
    {
        RedisReply watch(m_db, "WATCH " + getKeySetName(), REDIS_REPLY_STATUS);
        watch.checkStatusOK();
        multi();
        enqueue(std::string("SCARD ") + getKeySetName(), REDIS_REPLY_INTEGER);
        subscribe(m_db, getChannelName());
        bool succ = exec();
        if (succ) break;
    }

    RedisReply r(dequeueReply());
    setQueueLength(r.getReply<long long int>());
}

void ConsumerStateTable::pop(KeyOpFieldsValuesTuple &kco, std::string prefix)
{
    if (m_buffer.empty())
    {
        pops(m_buffer, prefix);
    }

    if (m_buffer.empty())
    {
        auto& values = kfvFieldsValues(kco);
        values.clear();
        kfvKey(kco).clear();
        kfvOp(kco).clear();
        return;
    }

    kco = m_buffer.front();
    m_buffer.pop_front();
}

void ConsumerStateTable::pops(std::deque<KeyOpFieldsValuesTuple> &vkco, std::string /*prefix*/)
{
    static std::string luaScript = loadLuaScript("consumer_state_table_pops.lua");

    static std::string sha = loadRedisScript(m_db, luaScript);

    RedisCommand command;
    command.format(
        "EVALSHA %s 2 %s %s: %d ''",
        sha.c_str(),
        getKeySetName().c_str(),
        getTableName().c_str(),
        POP_BATCH_SIZE);

    RedisReply r(m_db, command);
    auto ctx0 = r.getContext();
    vkco.clear();

    // if the set is empty, return an empty kco object
    if (ctx0->type == REDIS_REPLY_NIL)
    {
        return;
    }

    assert(ctx0->type == REDIS_REPLY_ARRAY);
    size_t n = ctx0->elements;
    vkco.resize(n);
    for (size_t ie = 0; ie < n; ie++)
    {
        auto& kco = vkco[ie];
        auto& values = kfvFieldsValues(kco);
        assert(values.empty());

        auto& ctx = ctx0->element[ie];
        assert(ctx->elements == 2);
        assert(ctx->element[0]->type == REDIS_REPLY_STRING);
        std::string key = ctx->element[0]->str;
        kfvKey(kco) = key;

        assert(ctx->element[1]->type == REDIS_REPLY_ARRAY);
        auto ctx1 = ctx->element[1];
        for (size_t i = 0; i < ctx1->elements / 2; i++)
        {
            FieldValueTuple e;
            fvField(e) = ctx1->element[i * 2]->str;
            fvValue(e) = ctx1->element[i * 2 + 1]->str;
            values.push_back(e);
        }

        // if there is no field-value pair, the key is already deleted
        if (values.empty())
        {
            kfvOp(kco) = DEL_COMMAND;
        }
        else
        {
            kfvOp(kco) = SET_COMMAND;
        }
    }
}

ConsumerKeySpaceTable::ConsumerKeySpaceTable(DBConnector *db, std::string tableName):
        ConsumerStateTable(db, tableName, 0)
{
    m_keyspace = "__keyspace@";

    m_keyspace += std::to_string(db->getDB()) + "__:" + tableName + ":*";

    psubscribe(m_db, m_keyspace);
    /* Don't read existing data implicitly here */
    setQueueLength(0);
}

void ConsumerKeySpaceTable::pop(KeyOpFieldsValuesTuple &kco, std::string prefix)
{
    if (m_buffer.empty())
    {
        pops(m_buffer, prefix);
    }

    if (m_buffer.empty())
    {
        auto& values = kfvFieldsValues(kco);
        values.clear();
        kfvKey(kco).clear();
        kfvOp(kco).clear();
        return;
    }

    kco = m_buffer.front();
    m_buffer.pop_front();
}

/*
 * handle redis keyspace pmessage notification
 */
void ConsumerKeySpaceTable::pops(std::deque<KeyOpFieldsValuesTuple> &vkco, std::string /*prefix*/)
{
    RedisReplyAsync r(getSubscribeDBC());
    // Keep fetching the reply object until all of them are read
    while(auto ctx0 = r.getContext())
    {
        KeyOpFieldsValuesTuple kco;
        // if the Key-space notification is empty, try next one.
        if (ctx0->type == REDIS_REPLY_NIL)
        {
            continue;
        }

        assert(ctx0->type == REDIS_REPLY_ARRAY);
        size_t n = ctx0->elements;

        //Expecting 4 elements for each keyspace pmessage notification
        if(n != 4)
        {
            SWSS_LOG_ERROR("invalid number of elements %lu for pmessage of %s", n, m_keyspace.c_str());
            continue;
        }
        // TODO: more validation and debug log
        auto ctx = ctx0->element[1];
        if(m_keyspace.compare(ctx->str))
        {
            SWSS_LOG_ERROR("invalid pattern %s returned for pmessage of %s", ctx->str, m_keyspace.c_str());
            continue;
        }

        ctx = ctx0->element[2];
        std::string table_key = ctx->str;
        size_t found = table_key.find(':');
        // Return if the format of key is wrong
        if (found == std::string::npos)
        {
            SWSS_LOG_ERROR("invalid keyspace notificaton %s", table_key.c_str());
            continue;
        }
        //strip off  the keyspace db string
        table_key = table_key.substr(found+1);

        found = table_key.find(':');
         // Return if the format of key is wrong
        if (found == std::string::npos)
        {
            SWSS_LOG_ERROR("invalid table key %s", table_key.c_str());
            continue;
        }
        // get the key in subscription table
        std::string key  = table_key.substr(found+1);

        ctx = ctx0->element[3];
        if (strcmp("del", ctx->str) == 0)
        {
            kfvKey(kco) = key;
            kfvOp(kco) = DEL_COMMAND;
        }
        else
        {
            if(!get(table_key, kfvFieldsValues(kco)))
            {
                SWSS_LOG_ERROR("Failed to get content for table key %s", table_key.c_str());
                continue;
            }
            kfvKey(kco) = key;
            kfvOp(kco) = SET_COMMAND;
        }
        vkco.push_back(kco);
    }
    return;
}

bool ConsumerKeySpaceTable::get(std::string key, std::vector<FieldValueTuple> &values)
{
    std::string hgetall_key("HGETALL ");

    hgetall_key += key;
    RedisReply r(m_db, hgetall_key, REDIS_REPLY_ARRAY);
    redisReply *reply = r.getContext();
    values.clear();

    if (!reply->elements)
        return false;

    if (reply->elements & 1)
        throw std::system_error(make_error_code(std::errc::result_out_of_range),
                           "Invalid redis reply");

    for (unsigned int i = 0; i < reply->elements; i += 2)
        values.push_back(std::make_tuple(reply->element[i]->str,
                                    reply->element[i + 1]->str));

    return true;
}

}
