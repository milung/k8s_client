:- module(k8s_client, [
    k8s_create_resource/6,      % +ApiGroup:atom, +Version:atom, +ResourceTypeName:atom, +Instanc:dict, -InstanceOut:dict, +Options:list
    k8s_delete_resource/5,      % +ApiGroup:atom, +Version:atom, +ResourceTypeName:atom, +Instance:dict/atom, +Options:list
    k8s_get_resource/6,         % +ApiGroup:atom, +Version:atom, +ResourceTypeName:atom, ?InstanceName:atomic, -Instance:dict, +Options:list
    k8s_resource_types/2,       % -ResourceTypes:list(dict), +Options:list
    k8s_update_resource/6,      % +ApiGroup:atom, +Version:atom, +ResourceTypeName:atom, +Instanc:dict, -InstanceOut:dict, +Options:list
    k8s_watch_resources/5,      % :Callback, +ApiGroup:atom, +Version:atom, +ResourceTypeName:atom, +Options:list
    k8s_watch_resources_async/6 % :Callback, +ApiGroup:atom, +Version:atom, +ResourceTypeName:atom, -StopWatcher, +Options:list
    ]).
%! <module> k8s_client module for accessing the kubernetes API server
%  Predicates for communicating with the kubernetes API server, with support to standard configuration options via `KUBECONFIG`, `~/.kube/config`, 
%  or from-pod-process access. 
%  
%  Example: 
%  ```prolog
%  ?- use(library(k8s_client)).
%  ?- k8s_get_resource(core, v1, pods, PodName, _, [k8s_namespace(myns)]).
%  PodName = "dex-567cdd88cd-dbv9x" ;
%  PodName = "envoy-proxy-599b679cf7-94764" ;
%  ...
%

:- set_prolog_flag(generate_debug_info, false).

:- use_module(library(yaml)).
:- use_module(library(http/http_open)).
:- use_module(library(http/json)).
:- use_module(library(http/http_client)).


:- dynamic
    cluster_resources/2,
    watcher_status/2.

:- meta_predicate 
    k8s_watch_resources(2, +, +, +, +),
    k8s_watch_resources_async(2, +, +, +, -, +),
    watch_modification_call(2, +, +, +, -, +, -),
    watch_resources_loop(2, +, +, +, +, +),
    watch_stream(2, +, +, +, +, +).

%%% PUBLIC PREDICATES %%%%%%%%%%%%%%%%%%%%%%%%%%

%! k8s_create_resource(+ApiGroup:atom, +Version:atom, +ResourceTypeName:atom, +Instanc:dict, -InstanceOut:dict, +Options:list) is semidet.
%  Creates a resource at the Kubernetes API and unifies the `InstanceOut` with the server response. `Options` are same as for the predicate 
%  `k8s_get_resource/6`
k8s_create_resource(ApiGroup, Version, ResourceTypeName, Instance, InstanceOut, Options) :-
    context_options(Options, Options1),
    resource_uri(ApiGroup, Version, ResourceTypeName, Uri, Options1),
    api_get(Uri, InstanceOut, [method(post), post(json(Instance)) |Options1 ]).

%! k8s_delete_resource(+ApiGroup:atom, +Version:atom, +ResourceTypeName:atom, +Instance:dict/atom, +Options:list) is semidet.
%  Delete a resource at the Kubernetes API. `Options` are same as for the predicate 
%  `k8s_get_resource/6`
k8s_delete_resource(ApiGroup, Version, ResourceTypeName, Instance, Options) :-
    (   is_dict(Instance)
    ->  InstanceName = Instance.metadata.name
    ;   InstanceName = Instance
    ),
    context_options(Options, Options1),
    resource_uri(ApiGroup, Version, ResourceTypeName, Uri0, Options1),
    atomic_list_concat([Uri0, InstanceName], '/', Uri),
    api_get(Uri, _, [method(delete) |Options1 ]).

%! k8s_get_resource(+ApiGroup:atom, +Version:atom, +ResourceTypeName:atom, -InstanceName:atomic, -Instance:dict, +Options:list) is nondet.
%  k8s_get_resource(+ApiGroup:atom, +Version:atom, +ResourceTypeName:atom, +InstanceName:atomic, -Instance:dict, +Options:list) is nondet.
%  Unifies `InstanceName` - and `Instance` with the object representing resource of the kubernetes API. If the `InstanceName` is not bound
%  then all instances are retrieved. `ApiGroup` is either the valid name of the Kubernetes API Group or the `core` atom.
%
%  The actual cluster address, context, and namespace is provided either options or loaded from the configuration. The below options are supported, 
%  in addition to options passed down to the `http_open/3` predicate: 
%  * `k8s_config(Config:dict)` - kubectl configuration - if not provided then the configuation is loaded by first succesfull of the following possibilities
%     - resolving and mergin config files specified by environment variable `KUBECONFIG` if existing;
%     - loading `~/.kube/config` file if existing;
%     - using pod service account if the process is executed from the pod.
%  * `k8s_context(Context:atom)` - name of the context to use from the `k8s_config` option. If not specified, then `current-context` property of `k8s_config`
%    is used as a default value
%  * `k8s_namespace(Namespace)` - the namespace from which to load resource instance(s). Shall be set to `all` if all namespaces shall be listed
%    (will fail if `InstanceName` is bound and resource is namespaced). If not specified then the namespace provided as part of the context or the 
%    `default` namespace will be used. 
%  * `k8s_resource_types_mode(Mode)` where  `Mode` is one of `cache` (default), `renew`, `remote`, `local`. Default to `local`. Used when resolving
%      if the resource is namespaced. See also `k8s_resource_types/2`
%  * `k8s_selectors(Selectors:list)` - list of selectors to apply for the resource retrieval(plain, not encoded form) 
%  * `k8s_query(Query:term)` - a query parameter to add to the REST call in form as specified by the predicate `uri_query_components/2`. This
%    option is concatenated if used multiple times
k8s_get_resource(ApiGroup, Version, ResourceTypeName, InstanceName, Instance, Options) :-
    % list retrieval 
    var(InstanceName),
    var(Instance), 
    context_options(Options, Options1),
    resource_uri(ApiGroup, Version, ResourceTypeName, Uri, Options1),
    api_get(Uri, List, Options1),
    member(Instance, List.items),
    ignore( InstanceName = Instance.get(metadata).name).
 k8s_get_resource(ApiGroup, Version, ResourceTypeName, InstanceName, Instance, Options) :-
    % list retrieval 
    nonvar(InstanceName),
    var(Instance),
    context_options(Options, Options1),
    resource_uri(ApiGroup, Version, ResourceTypeName, Uri0, Options1),
    atomic_list_concat([Uri0, InstanceName], '/', Uri),
    api_get(Uri, Instance, Options1).

%! k8s_resources(-ResourceTypes:list(dict), +Options) is semidet.
%  Unifies the `ResourceTypes` with list of resource types available on the cluster. Primary used for resource 
%  discovery.
%  `Options˙ are same as for the `k8s_get_resource/6` with extra option: 
%   * `k8s_resource_types_mode(Mode)` where  `Mode` is one of `cache` (default), `renew`, `remote`, `local`. In the 
%     standard mode, the resources are cached per cluster and retrieved from the cache if available. 
%     If `renew` or `remote˙ mode is requested then the resource list is retrieved directly from the 
%     API server. The 'remote' and `local` mode leaves the cache untouched. The caching significantly speed up consequent
%     operation with kubernetes resources
k8s_resource_types(ResourceTypes, Options) :-
    select_option(k8s_resource_types_mode(Caching), Options, Options0, cache),
    Caching \= renew, 
    Caching \= remote,
    context_options(Options0, Options1),
    option(k8s_context(ContextName), Options1),
    option(k8s_config(Config), Options1),
    config_get_context(Config, ContextName, Context),
    ClusterName = Context.cluster,
    cluster_resources(ClusterName, ResourceTypes),
    !.
k8s_resource_types(ResourceTypes, Options) :-
    select_option(k8s_resource_types_mode(Caching), Options, Options0, cache),
    Caching \= local,
    context_options(Options0, Options1),
    api_resources_core( Core, Options1),
    api_resources_groups( Grouped, Options1),
    append(Core, Grouped, ResourceTypes),
    % cache results
    (   member(Caching, [cache, renew])
    ->  option(k8s_context(ContextName), Options1),
        option(k8s_config(Config), Options1),
        config_get_context(Config, ContextName, Context),
        ClusterName = Context.cluster,
        retractall(cluster_resources(ClusterName,_)),
        asserta(cluster_resources(ClusterName, ResourceTypes))
    ;   true
    ), 
    !.

%! k8s_update_resource(+ApiGroup:atom, +Version:atom, +ResourceTypeName:atom, +Instanc:dict, -InstanceOut:dict, +Options:list) is semidet.
%  Updates a resource at the Kubernetes API and unifies the `InstanceOut` with the server response. Options are same as for the predicate 
%  `k8s_get_resource/6`
k8s_update_resource(ApiGroup, Version, ResourceTypeName, Instance, InstanceOut, Options) :-
    context_options(Options, Options1),
    resource_uri(ApiGroup, Version, ResourceTypeName, Uri0, Options1),
    atomic_list_concat([Uri0, Instance.metadata.name], '/', Uri),
    api_get(Uri, InstanceOut, [method(put), post(json(Instance)) |Options1 ]).

%! k8s_watch_resources(:Callback, +ApiGroup:atom, +Version:atom, +ResourceTypeName:atom, +Options:list) is det
%  This predicate watches the changes of the resource list specified by the arguments and call `call(:Goal, ChangeType:atom, ResourceInstance:dict)`
%  for each modification in the list. The `ChangeType` is one of the `added`, `modified`, `deleted` atom. Initial list of instances 
%  is provided as sequence of `added` callbacks after the call of this predicate. 
%
%  The call is blocking the caller thread.
% 
%  `Options` are same as for the `k8s_get_resource/6` with extra option:
%  * `k8s_resource_version(ResourceVersion:atom)` - if specified the initial list is retrieved for the changed since the specified resource version. 
%    This option is used primary for internal purposes, and can be reset back to 0. 
%  * `heartbeat_callback(:Callback)` - the `Callback` is invoked each time there is a change, error, or channel timeout occuring during watching the resource.
%    This may be usefull for healthiness check of the controller. While the loop tends to be robust to typical issue of the errors during watching the callback
%    may implement additional level of robustness. The failure of the callback is ignored.
k8s_watch_resources(Callback, ApiGroup, Version, ResourceTypeName, Options) :-
    select_option(k8s_resource_version(ResourceVersion), Options, Options1, 0),
    watch_resources_loop(Callback, ApiGroup, Version, ResourceTypeName, state(ResourceVersion, []), Options1).

%! k8s_watch_resources_async(:Callback, +ApiGroup:atom, +Version:atom, +ResourceTypeName:atom, -StopWatcher, +Options:list) is det
%  Forks the separated backgrond thread in which the predicate  'k8s_watch_resources/5` is execute. THe background thread can be finished and joined 
%  by calling `call(StopWatcher)`. Other arguments are same as for the 'k8s_watch_resources/5`. Be aware that the `Callback` is invoked from the 
%  different thread than the thread calling this predicate.
k8s_watch_resources_async(Callback, ApiGroup, Version, ResourceTypeName, k8s_client:watcher_exit(Id), Options) :-
    thread_create(
        k8s_watch_resources(Callback, ApiGroup, Version, ResourceTypeName, [watcher_id(Id), timeout(1) | Options]),
        Id, 
        [   at_exit(retractall(watcher_status(Id, _)))
        ]
    ). 

%%% PRIVATE PREDICATES %%%%%%%%%%%%%%%%%%%%%%%%%

api_get(Url, Reply) :-
    api_get(Url, Reply, []).

api_get( Url, Reply, Options) :-
    config_connection_options( Url, UriComponents, Options, Options1),    
    uri_components(Uri, UriComponents),
    http_get( Uri, Reply, [ json_object(dict) |Options1]).

api_resources_core(Resources, Options) :-
    api_get(api, Versions, Options),
    foldl(api_resources_core_(Options), Versions.versions, [], Resources).

api_resources_core_(Options, Version, In, Out) :-
    atomic_list_concat([api, Version], '/', Url),
    api_get(Url, Resources, Options),
    maplist(put_dict( _{group: core, version: Version}), Resources.resources, Resources1),
    append(In, Resources1, Out). 

api_resources_groups(Resources, Options) :-
    api_get(apis, Groups, Options),
    foldl(api_resources_groups_(Options), Groups.groups, [], Resources).

api_resources_groups_(Options, Group, In, Resources) :-
    Preferred = Group.preferredVersion.version,
    api_resources_groups_(Options, Preferred, Group.preferredVersion, In, Resources).

api_resources_groups_(Options, Preferred, GroupVersion, In, Out) :-
    atomic_list_concat([apis, GroupVersion.groupVersion], '/', Url),
    api_get(Url, GroupResources, Options),
    ( Preferred = GroupVersion.version
    ->  IsPreferred = true
    ;   IsPreferred = false
    ),
    maplist(
        put_dict( _{group: groupVersion, version: GroupVersion.version,  isPreferred: IsPreferred }), 
        GroupResources.resources, 
        Resources1),
    append(In, Resources1, Out).

atomic_eq( Left, Right) :-
    nonvar(Left),
    atom_string(LeftA, Left), 
    atom_string(LeftA, Right),
    !.
atomic_eq( Left, Right) :-
    nonvar(Right),    
    atom_string(RightA, Right), 
    atom_string(RightA, Left),
    !.

base64_certificate(Cert64, Certificate) :-
    base64(Cert, Cert64),
    atom_codes(Cert, CertCodes),
    open_codes_stream(CertCodes, CertStream),
    load_certificate(CertStream, Certificate).

config_connection_options(ResourceUrl, Server, OptionsIn, OptionsOut) :-
    (   select_option(k8s_config(Config), OptionsIn, Options1)
    ->  true
    ;   load_config(Config),
        Options1 = OptionsIn
    ), 
    (   select_option(k8s_context(ContextName), Options1, Options2)
    ->  true
    ;   config_current_context(Config, ContextName),
        Options2 = Options1
    ), 
    config_connection_options(ResourceUrl, Config, ContextName, Server, Options2, OptionsOut).

config_connection_options(ResourceUrl, Config, ContextName, ServerUriComponents, OptionsIn, OptionsOut) :-
    config_get_context(Config, ContextName, Ctx),
    atom_string(ClusterName, Ctx.cluster),
    config_get_cluster(Config, ClusterName, Cluster ),
    config_connection_resource_uri(ResourceUrl, Cluster.server, UriComponents),
    config_connections_queries(UriComponents, OptionsIn, ServerUriComponents, Options0),
    config_cluster_options(Cluster, Options0, Options1),
    config_client_options(Config, Ctx, Options1, OptionsOut),
    !. 

config_connections_queries(UriCompnentsIn, OptionsIn, UriCompnentsOut, OptionsOut) :-
    uri_data(search, UriCompnentsIn, Search),
    (   var(Search)
    ->  Query0 = []
    ;   uri_query_components(Search, Query0)
    ),
    config_connections_queries_(Query0, Queries, OptionsIn, OptionsOut), 
    (   Queries = []
    ->  UriCompnentsIn = UriCompnentsOut
    ;   uri_query_components(QueriesSegment, Queries),
        uri_data(search, UriCompnentsIn, QueriesSegment, UriCompnentsOut)
    ).

config_connections_queries_(QueriesIn, QueriesOut, OptionsIn, OptionsOut) :-
    select_option( k8s_query(Query), OptionsIn, Options),
    is_list(Query), 
    append(Query, QueriesIn, Queries),
    !,
    config_connections_queries_( Queries, QueriesOut, Options, OptionsOut).
 config_connections_queries_(QueriesIn, QueriesOut, OptionsIn, OptionsOut) :-
    select_option( k8s_query(Query), OptionsIn, Options),
    !,
    config_connections_queries_( [Query | QueriesIn], QueriesOut, Options, OptionsOut).
 config_connections_queries_(QueriesIn, QueriesOut, OptionsIn, OptionsOut) :-  
    select_option( k8s_resource_version(ResourceVersion), OptionsIn, Options),
    ResourceVersion \= 0,
    !,
    config_connections_queries_( [ resourceVersion = ResourceVersion | QueriesIn], QueriesOut, Options, OptionsOut).
 config_connections_queries_(QueriesIn, QueriesOut, OptionsIn, OptionsOut) :-
    select_option( k8s_selectors(Selectors), OptionsIn, Options),
    (   is_list(Selectors)
    ->  atomic_list_concat(Selectors, ',', SelectorsText)
    ;   SelectorsText = Selectors
    ),
    !,
    config_connections_queries_( [ labelSelector = SelectorsText | QueriesIn], QueriesOut, Options, OptionsOut).
 config_connections_queries_(Queries, Queries, Options, Options).

config_connection_resource_uri(ResourceUrl, ServerName, UriComponents) :-
    atom_string(Server, ServerName),
    uri_components(Server, UriComponents0),
    uri_data(path, UriComponents0, Path),
    directory_file_path(Path, ResourceUrl, FullPath),
    uri_data(path, UriComponents0, FullPath, UriComponents).

config_ca_options(Cluster, OptionsIn, [ cacerts([certificate(CaCert)])| OptionsIn ]) :-
    base64_certificate( Cluster.get('certificate-authority-data'), CaCert),
    !.
 config_ca_options(Cluster, OptionsIn, [ cacerts([file(CaCert)])| OptionsIn ]) :-
    Cluster.get('certificate-authority') = CaCert,
    !.
 config_ca_options(_, _, _) :-
    print_message(error, kubernetes(unsupported_config, cluster)),
    fail.

config_client_options(Config, Ctx, OptionsIn, [ certificate_key_pairs([ClientCert-ClientKey]) | OptionsIn]) :-
    atom_string(UserName, Ctx.user), 
    config_get_user(Config, UserName, User),
    base64(ClientCert, User.get('client-certificate-data')),
    base64(ClientKey, User.get('client-key-data')),
    !.
 config_client_options(Config, Ctx, OptionsIn, [ certificate_file(ClientCert), certificate_file(ClientKey) | OptionsIn]) :-
    atom_string(UserName, Ctx.user), 
    config_get_user(Config, UserName, User),
    base64(ClientCert, User.get('client-certificate')),
    base64(ClientKey, User.get('client-key')),
    !.
  config_client_options(Config, Ctx, OptionsIn, [ authorization(bearer(Token)) | OptionsIn]) :-
    atom_string(UserName, Ctx.user), 
    config_get_user(Config, UserName, User),
    atom_string(Token, User.get(token)),
    !.
  config_client_options(_, _, _, _) :-
    print_message(error, kubernetes(unsupported_config, user)),
    fail.

config_cluster_options(Cluster, OptionsIn, OptionsOut) :-
    config_ca_options(Cluster, OptionsIn, Options0),
    (   Proxy = Cluster.get(proxy)
    ->  Options1 = [proxy(Proxy) | Options0 ]
    ;   Options1 = Options0
    ),
    (   Cluster.get('insecure-skip-tls-verify') = true
    ->  OptionsOut = [ cert_verify_hook(cert_accept_any) | Options1]
    ;   OptionsOut = Options1).

config_current_context(Context) :-
    load_config(Cfg),
    config_current_context(Cfg, Context).

config_current_context(Cfg, Context) :-
    atom_string(Context, Cfg.get('current-context')).

config_get_cluster(Config, ClusterName, Cluster) :-
    member(ClusterDict, Config.clusters),
    atom_string(ClusterName, ClusterDict.name),
    Cluster = ClusterDict.cluster.
 
config_get_context(Config, ContextName, Context) :-
    member(ContextDict, Config.contexts),
    atom_string(ContextName, ContextDict.name),
    Context = ContextDict.context.

config_get_user(Config, UserName, User) :-
    member(UserDict, Config.users),
    atom_string(UserName, UserDict.name),
    User = UserDict.user.
   
context_options( OptionsIn, OptionsOut) :-
    (   option(k8s_config(Config), OptionsIn)
    ->  OptionsIn = Options1
    ;   load_config(Config),
        Options1 = [ k8s_config(Config) | OptionsIn ]
    ), 
    (   option(k8s_context(ContextName), Options1)
    ->  Options1 = Options2
    ;   config_current_context(Config, ContextName),
        Options2 = [ k8s_context(ContextName) | Options1 ]
    ), 
    (   option(k8s_namespace(_), Options2)
    ->  Options2 = OptionsOut
    ;   config_current_context(Config, ContextName),
        config_get_context(Config, ContextName, Context),
        dict_get_default(Context, namespace, "default", Namespace),
        OptionsOut = [ k8s_namespace(Namespace) | Options2 ]
    ),
    !.  

dict_get_default(Dict, Key, Default, Value) :-
    Dict.get(Key) = Value
    -> true
    ;  Value = Default.

is_resource_namespaced( ApiGroup, Version, ResourceTypeName, Options ) :-
    (   option(k8s_resource_types_mode(_), Options)
    ->  LocalOptions = Options  % respect caching options
    ;   LocalOptions = [ k8s_resource_types_mode(local) | Options ] % or avoid loading all resource types side effect
    ),
    % get list of the resource types
    (   k8s_resource_types(ResourceTypes, LocalOptions)
    ->  true
    ;   (   ApiGroup = core
        ->  api_resources_core_(Options, Version, [], ResourceTypes)
        ;   atomic_list_concat([ApiGroup, Version], '/', GroupVersion),
            api_resources_groups_(Options, Version, _{ version: Version, groupVersion: GroupVersion}, [], ResourceTypes)
        )
    ),
    % check if resource is naespaced 
    member( ResourceType, ResourceTypes),
    atomic_eq(ResourceTypeName, ResourceType.name),
    atomic_eq(ApiGroup, ResourceType.group),
    atomic_eq(Version, ResourceType.version),
    !,
    true = ResourceType.namespaced.

load_and_merge_config_file(Path, ConfigIn, ConfigOut) :-
    path_to_posix(Path, PathPx),
    yaml_read(PathPx, ConfigDict),
    ConfigIn1 = _{ 
        clusters:[], users: [], contexts: [], 
        'current-context': "", preferences: _{}
    }.put(ConfigIn),
    ConfigDict1 = _{ 
        clusters:[], users: [], contexts: [], 
        'current-context': ConfigIn1.'current-context', 
        preferences: ConfigIn1.preferences
        }.put(ConfigDict),
    append(ConfigIn1.clusters, ConfigDict1.clusters, Clusters),
    append(ConfigIn1.users, ConfigDict1.users, Users),
    append(ConfigIn1.contexts, ConfigDict1.contexts, Contexts),
    ConfigOut = 
        _{
            apiVersion: v1,
            kind: 'Config', 
            preferences: ConfigDict1.preferences,
            clusters: Clusters,
            users: Users, 
            contexts: Contexts, 
            'current-context': ConfigDict1.'current-context'
        },
    !.

load_config(ConfigDict) :- % KUBECONFIG variant
    getenv('KUBECONFIG', Path),
    (   current_prolog_flag(windows, true)
    ->  atomic_list_concat(Files, ';', Path)
    ;   atomic_list_concat(Files, ':', Path)
    ),
    foldl(load_and_merge_config_file, Files, _{}, ConfigDict),
    print_message(informational, kubernetes(config_loaded, kubeconfig)),
    !.
 load_config(ConfigDict) :- % ~/.kube/config variant
    (   getenv('USERPROFILE', HomePath)
    ;   getenv('HOME', HomePath)
    ),
    path_to_posix(HomePath, HomePathPx),
    directory_file_path(HomePathPx, '.kube/config', ConfigPath),
    exists_file(ConfigPath),    
    yaml_read(ConfigPath, ConfigDict),
    print_message(informational, kubernetes(config_loaded, user_config)),

    !.

 load_config(ConfigDict) :- % access api from pod
    exists_file('/var/run/secrets/kubernetes.io/serviceaccount/token'),
    exists_file('/var/run/secrets/kubernetes.io/serviceaccount/ca.crt'),
    exists_file('/var/run/secrets/kubernetes.io/serviceaccount/namespace'),
    read_file_to_string('/var/run/secrets/kubernetes.io/serviceaccount/token', Token, []),
    read_file_to_string('/var/run/secrets/kubernetes.io/serviceaccount/namespace', Namespace, []),
    ConfigDict = _{
        apiVersion: v1,
            kind: 'Config', 
            clusters: [
                _{ 
                    name: "default-api",
                    cluster: _{
                        'certificate-authority': "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
                        server: "https://kubernetes.default.svc"
                    }
                }
            ],
            users: [
                _{
                    name: "service-account",
                    user: _{
                        token: Token
                    }
                }
            ],
            contexts: [
                _{
                    name: "from-pod",
                    context: _{
                        cluster: "default-api",
                        user: "service-account",
                        namespace: Namespace
                    }
                }
            ], 
        'current-context': "from-pod"
    },    
    print_message(informational, kubernetes(config_loaded, pod)),
    !.

noop_healtz.

path_to_posix(Path, Posix) :-
    atomic_list_concat(Segments, '\\', Path),
    atomic_list_concat(Segments, '/', Posix).

prolog:message(kubernetes(unsupported_config, cluster)) -->
    ['Kubernetes: Configuration of the cluster is not supported'].
 prolog:message(kubernetes(unsupported_config, user)) -->
    ['Kubernetes: Configuration of the user '].
 prolog:message(kubernetes(watcher_exited, Resource)) -->
    ['Kubernetes: Watching of the resources type `~p` exited '- Resource].
 prolog:message(kubernetes(watcher_update_failure, Goal, Id)) -->
    ['Kubernetes: Calling the goal ~p from resource controller for resource ~p modification failed'- [Goal, Id]].
 prolog:message(kubernetes(config_loaded, kubeconfig)) -->
    ['Kubernetes: Configuration taken using the environment variable KUBECONFIG'].
 prolog:message(kubernetes(config_loaded, user_config)) -->
    ['Kubernetes: Configuration taken the users home directory'].
 prolog:message(kubernetes(config_loaded, pod)) -->
    ['Kubernetes: Configuration taken from the kubernetes pod service account'].
 prolog:message(kubernetes(watch_modification, Change)) -->
    { atom_json_dict(Json, Change, [as(atom), width(0)]) },
    ['Kubernetes: Modification of resources detected: ~p' - [Json] ].

resource_uri(ApiGroup, Version, ResourceTypeName, Uri, Options) :-
    option(k8s_namespace(all), Options, all),
    (   ApiGroup = core
    ->  atomic_list_concat([api, Version, ResourceTypeName], '/', Uri)
    ;   atomic_list_concat([apis, ApiGroup, Version, ResourceTypeName ], '/', Uri)
    ),
    !.
 resource_uri(ApiGroup, Version, ResourceTypeName, Uri, Options) :-
    select_option(k8s_namespace(Namespace), Options, Options1),
    (   is_resource_namespaced(ApiGroup, Version, ResourceTypeName, Options)
    ->  (   ApiGroup = core
        ->  atomic_list_concat([api, Version, namespaces, Namespace, ResourceTypeName], '/', Uri)
        ;   atomic_list_concat([apis, ApiGroup, Version, namespaces, Namespace, ResourceTypeName], '/', Uri)
        )
    ;   resource_uri(ApiGroup, Version, ResourceTypeName, Uri, [ k8s_namespace(all) | Options1 ])
    ),
    !.

watch_modification_call(_, Id, _, Version, Version, KnownResources, KnownResources) :-
    watcher_status(Id, exit_request), % just exit if exist is requested
    !.
 watch_modification_call(_, _, error(timeout_error(read, _), _) , State, State) :- !. % just continue listening
 watch_modification_call(_, _, error(_, _), State, _) :- % rethrow other errors
    throw(error(k8s_watcher_error(connection_broken), State)).
 watch_modification_call(_, _, end_of_file, State, _) :- % need reconnect if stream is ended
    throw(error(k8s_watcher_error(connection_broken), State)).
 watch_modification_call(_, _, Change, state(_, R), state(0, R)) :-
    Change.get(type) =  "ERROR" . % if no bookmark was sent and only old resources available then reset version
 watch_modification_call(_, _, Change, state(_, R), state(Version, R)) :-
    Change.get(type) =  "BOOKMARK",
    Version = Change.object.metadata.resourceVersion.
  watch_modification_call(Goal, _, Change, state(_, R), state(Version, [Resource | Rest])) :-
    memberchk(Change.get(type), [ "ADDED", "MODIFIED"]),    
    dict_get_default(Change.object.metadata, namespace, [], ResourceNamespace),
    ResourceName =  Change.object.metadata.name,
    Version = Change.object.metadata.resourceVersion,
    (   select(resource(ResourceNamespace, ResourceName, OldVersion), R, Rest)
    ->  (   OldVersion = Version
        ->  true
        ;   call(Goal, modified, Change.object)
        )
    ;   call(Goal, added, Change.object),
        Rest = R
    ),
    Resource = resource(ResourceNamespace, ResourceName, Version).
 watch_modification_call(Goal, _, Change,  state(_, R), state(Version, Rest)) :-
    dict_get_default(Change.object.metadata, namespace, [], ResourceNamespace),
    ResourceName =  Change.object.metadata.name,
    Version = Change.object.metadata.resourceVersion,
    (   select(resource(ResourceNamespace, ResourceName, _), R, Rest)
    ->  call(Goal, deleted, Change.object)
    ;   Rest = R
    ).
 watch_modification_call(Goal, Id, _, State, State) :- 
    print_message(error, kubernetes(watcher_update_failure,Goal, Id)).


watch_resources_loop(_, _, _, ResourceTypeName, state(Version, _),  Options) :-  % special handling to exit the async loop
    (   option(watcher_id(Id), Options),
        watcher_status(Id, exit_request)
    ->  (   print_message(informational, kubernetes(watcher_exited, ResourceTypeName)),
            thread_exit(resourceVersion(Version))
        )
    ),
    !.
 watch_resources_loop(Callback, ApiGroup, Version, ResourceTypeName, State, Options) :-
    context_options([k8s_query(watch=1), k8s_query(allowWatchBookmarks=true) | Options], Options1),
    resource_uri(ApiGroup, Version, ResourceTypeName, Url, Options1),
    config_connection_options( Url, UriComponents, Options1, Options2),
    uri_components(Uri, UriComponents),
    http_open( Uri, Stream, Options2),
    (   option(watcher_id(Id), Options)
    ->  retractall(watcher_status(Id, running(_))),        
        asserta(watcher_status(Id, running(Stream)))
    ;   Id = []
    ),  
    (   option(heartbeat_callback(HeartCallback), Options)
    ->  true
    ;   HeartCallback = noop_healtz
    ),
      
    !,
    catch(
        watch_stream(Callback, HeartCallback, Stream, Id, State, State1),
        error(k8s_watcher_error(connection_broken), State1), 
        true
    ),
    sleep(1), % reduce CPU load in case of persistent connection error
    !, % cut here to avoid recursion stack on async loop
    watch_resources_loop(Callback, ApiGroup, Version, ResourceTypeName, State1, Options).

watch_stream(_, _, _, Id, State, State) :-
    watcher_status(Id, exit_request),
    !.
watch_stream(Goal, HeartCallback, Stream, Id, StateIn, StateOut) :-
    ignore(HeartCallback),
    catch(
        (   peek_string(Stream, 4, _),
            json_read_dict(Stream, Change, [end_of_file(end_of_file)]),
            print_message(informational, kubernetes(watch_modification, Change))
        ),
        Error,
        Change = Error
    ),
    watch_modification_call(Goal, Id, Change, StateIn, State0),
    !,
    watch_stream(Goal, HeartCallback, Stream, Id, State0, StateOut). 


watcher_exit(Id) :-
 watcher_exit(Id, _).

watcher_exit(Id, Status) :-
    (   retract(k8s_client:watcher_status(Id, running(Stream)))
    ->  assertz(k8s_client:watcher_status(Id, exit_request)),
        retractall(k8s_client:watcher_status(Id, running(_))),
        close(Stream)
    ;   assertz(k8s_client:watcher_status(Id, exit_request))
    ),
    thread_join(Id, Status),
    ignore(retractall(k8s_client:watcher_status(Id,_))).