-module(ot_elastic_exporter).

-export([init/1, export/2]).
-include_lib("opentelemetry_api/include/opentelemetry.hrl").

init(Opts) ->
    Kubernetes =
        case os:getenv("KUBERNETES_NAMESPACE") of
            false ->
                #{};
            Namespace ->
                #{<<"namespace">> => list_to_binary(Namespace),
                  <<"node">> => #{
                                  <<"name">> => list_to_binary(os:getenv("KUBERNETES_NODE_NAME"))
                                 },
                  <<"pod">> => #{
                                 <<"uid">> => list_to_binary(os:getenv("KUBERNETES_POD_UID")),
                                 <<"name">> => list_to_binary(os:getenv("KUBERNETES_POD_NAME"))
                                }
                 }
        end,

    {ok, Hostname} = inet:gethostname(),

    [Architecture, _, Platform | _] = string:split(erlang:system_info(system_architecture), "-", all),

    System = #{
               <<"architecture">> => list_to_binary(Architecture),
               <<"hostname">> => list_to_binary(Hostname),
               <<"platform">> => list_to_binary(Platform),
               <<"kubernetes">> => Kubernetes
              },

    {ok, Application} = application:get_application(?MODULE),

    Agent = #{
              <<"name">> => atom_to_binary(Application, unicode),
              <<"version">> => list_to_binary(element(3, lists:keyfind(Application, 1, application:loaded_applications())))
             },

    Language = #{
                 <<"name">> => <<"Erlang">>,
                 <<"version">> => list_to_binary(erlang:system_info(otp_release))
                },

    Runtime = #{
                <<"name">> => list_to_binary(erlang:system_info(machine)),
                <<"version">> => list_to_binary(erlang:system_info(version))
               },

    [{Name, Vsn, _, _}] = release_handler:which_releases(permanent),
    % service name should be match [a-zA-Z _-]*
    Name2 = binary:replace(list_to_binary(Name), <<"/">>, <<"_">>),

    Service = #{
                <<"name">> => Name2,
                <<"version">> => list_to_binary(Vsn),
                <<"agent">> => Agent,
                <<"language">> => Language,
                <<"runtime">> => Runtime
               },
    Metadata = #{
                 <<"system">> => System,
                 <<"service">> => Service
                },
    M = jsone:encode(#{<<"metadata">> => Metadata}),
    {ok, Opts#{metadata => <<M/binary, "\n">>}}.

export(SpansTid, #{metadata := Metadata} = Opts) ->
    ServerURL = maps:get(server_url, Opts, <<"http://apm-server:8200">>),
    RequestSize = maps:get(api_request_size, Opts, 724*1024),
    Output =
        ets:foldr(
          fun(Span, Acc) ->
            Encoded = jsone:encode(format_span(Span)),
            [<<Encoded/binary, "\n">> | Acc]
          end, [], SpansTid),

    case Output of
        [] ->
            ok;
        _ ->
            SpansList = split_spans(RequestSize - byte_size(Metadata), Output),
            send_spans_list(ServerURL, Metadata, SpansList)
    end.

split_spans(_, []) ->
    [];
split_spans(Limit, Spans) ->
    {H, T} = take_spans(Limit, Spans),
    [H|split_spans(Limit, T)].

take_spans(Size, []) when Size > 0 ->
    {[], []};
take_spans(Size, [H|T]) when Size > 0 ->
    {T1, Rest} = take_spans(Size - byte_size(H), T),
    {[H|T1], Rest};
take_spans(_, Spans) ->
    {[], Spans}.


send_spans_list(_, _, []) ->
    ok;
send_spans_list(ServerURL, Metadata, [H|T]) ->
    send_spans(ServerURL, [Metadata|H]),
    send_spans_list(ServerURL, Metadata, T).

send_spans(ServerURL, Spans) ->
    Body = lists:foldr(fun(X, Acc) -> <<X/binary, Acc/binary>> end, <<"">>, Spans),
    case hackney:request(
           post,
           [ServerURL, <<"/intake/v2/events">>],
           [{<<"Content-Type">>, <<"application/x-ndjson">>}],
           Body,
           [])
    of
        {ok, 202, _, _} ->
            ok;
        {ok, Code, _, BodyRef} ->
            logger:error("request apm error, code: ~p, body: ~p", [Code, hackney:body(BodyRef)]),
            ok;
        {error, timeout} ->
            logger:error("APM Server timeout"),
            ok
    end.

format_span(
  #span{
     name = Name,
     span_id = Id,
     trace_id = TraceId,
     parent_span_id = ParentSpanId,
     kind = Kind,
     start_time = StartTime,
     end_time = EndTime
    }) ->
    Span =
        #{<<"id">> => integer_to_binary(Id, 16),
          <<"trace_id">> => integer_to_binary(TraceId, 16),
          <<"timestamp">> => wts:to_absolute(StartTime),
          <<"duration">> => wts:duration(StartTime, EndTime) / 1000.0,
          <<"name">> => Name,
          <<"type">> => atom_to_binary(Kind, unicode)
         },
    case ParentSpanId of
        undefined ->
            #{<<"transaction">> => Span#{<<"span_count">> => #{<<"started">> => 0}}};
        _ ->
            #{<<"span">> => Span#{<<"parent_id">> => integer_to_binary(ParentSpanId, 16)}}
    end.
