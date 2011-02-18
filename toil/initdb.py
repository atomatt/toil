"""
'Executable' module to initialise a database for use as a jobqueue.
"""

import sys
import couchdb


TASK_FILTER_JS = '''
function(doc, req) {
    names={};
    for each (name in req.query.name.split(',')) {
        names[name] = true;
    }
    id = doc._id.split('~');
    return (id[0] == 'toil.task') && names[id[1]] && !doc._deleted && !doc.claimed && !doc.paused;
}
'''


RESPONSE_FILTER_JS = '''
function(doc, req) {
    return doc._id == req.query.docid && !doc._deleted;
}
'''


TASK_FILTER = '''
fun({Doc}, {Req}) ->
    {Query} = couch_util:get_value(<<"query">>, Req),
    Names = string:tokens(binary_to_list(couch_util:get_value(<<"name">>, Query)), ","),
    case couch_util:get_value(<<"_deleted">>, Doc) of
        undefined ->
            Id = string:tokens(binary_to_list(couch_util:get_value(<<"_id">>, Doc)), "~"),
            case lists:nth(1, Id) of
                "toil.task" ->
                    case couch_util:get_value(<<"claimed">>, Doc) of
                        undefined ->
                            case couch_util:get_value(<<"paused">>, Doc) of
                                undefined ->
                                    lists:member(lists:nth(2, Id), Names);
                                _ -> false
                            end;
                        _ -> false
                    end;
                _ -> false
            end;
        _ -> false
    end
end.
'''

RESPONSE_FILTER = '''
fun({Doc}, {Req}) ->
    {Query} = couch_util:get_value(<<"query">>, Req),
    DocId = couch_util:get_value(<<"docid">>, Query),
    case couch_util:get_value(<<"_id">>, Doc) of
        DocId -> true;
        _ -> false
    end
end.
'''

ACTIVE_TASKS = '''
fun({Doc}) ->
    case proplists:get_value(<<"paused">>, Doc) of
        undefined ->
            Emit(proplists:get_value(<<"_id">>, Doc),
                 proplists:get_value(<<"_rev">>, Doc));
        _ ->
            nothing
    end
end.
'''

CLAIMED_TASKS = '''
fun({Doc}) ->
    case proplists:get_value(<<"claimed">>, Doc) of
        undefined ->
            nothing;
        Claimed ->
            Emit(proplists:get_value(<<"_id">>, Doc), Claimed)
    end
end.
'''

SUSPENDED_TASKS = '''
fun({Doc}) ->
    case proplists:get_value(<<"paused">>, Doc) of
        undefined ->
            nothing;
        _ ->
            {Error} = lists:last(proplists:get_value(<<"errors">>, Doc)),
            Emit(proplists:get_value(<<"_id">>, Doc),
                 proplists:get_value(<<"error">>, Error))
    end
end.
'''


def funcesc(s):
    return s.strip().replace('"', '\\"')


DESIGN_DOC = '''
{
    "_id": "_design/toil",
    "language": "erlang",
    "filters": {
        "task": "%s",
        "response": "%s"
    },
    "views": {
        "active_tasks": {"map": "%s"},
        "claimed_tasks": {"map": "%s"},
        "suspended_tasks": {"map": "%s"}
    }
}
''' % (funcesc(TASK_FILTER), funcesc(RESPONSE_FILTER), funcesc(ACTIVE_TASKS),
       funcesc(CLAIMED_TASKS), funcesc(SUSPENDED_TASKS))


def main():
    dburl, = sys.argv[1:]
    db = couchdb.Database(dburl)
    db.resource.post(body=DESIGN_DOC, headers={'Content-Type': 'application/json'})


if __name__ == '__main__':
    main()
