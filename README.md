# Kubernetes API prolog client

The prolog module and predicates for communicating with the kubernetes API server,
with support of standard configuration options via `KUBECONFIG`, `~/.kube/config`,
and support for accessing the API from the k8s pods.

## USAGE

Use the one of the exported predicates for CRUD operations or to watchi the resource
lists. See [Kubernetes API concepts](https://kubernetes.io/docs/reference/using-api/api-concepts/)
for details about basic concept used in this module.

_Example of usage:_

```prolog
?- use(library(k8s_client)).
?- k8s_get_resource(core, v1, pods, PodName, _, [k8s_namespace(myns)]).
PodName = "dex-567cdd88cd-dbv9x" ;
PodName = "envoy-proxy-599b679cf7-94764" ;
```

## Exported predicates from the module `k8s_client`

### `k8s_create_resource(+ApiGroup:atom, +Version:atom, +ResourceTypeName:atom, +Instance:dict, -InstanceOut:dict, +Options:list)` is semidet

Creates a resource at the Kubernetes API and unifies the `InstanceOut` with the server response. `Options` are same as for the predicate
 `k8s_get_resource/6`

### `k8s_delete_resource(+ApiGroup:atom, +Version:atom, +ResourceTypeName:atom, +Instance:dict/atom, +Options:list)` is semidet

Deletes a resource at the Kubernetes API. `Options` are same as for the predicate `k8s_get_resource/6`

### `k8s_get_resource(+ApiGroup:atom, +Version:atom, +ResourceTypeName:atom, -InstanceName:atomic, -Instance:dict, +Options:list)` is nondet

### `k8s_get_resource(+ApiGroup:atom, +Version:atom, +ResourceTypeName:atom, +InstanceName:atomic, -Instance:dict, +Options:list)` is nondet

Unifies `InstanceName` - and `Instance` with the object representing resource of the kubernetes API. If the `InstanceName` is not bound
then all instances are retrieved. `ApiGroup` is either the valid name of the Kubernetes API Group or the `core` atom.

The actual cluster address, context, and namespace is provided either options or loaded from the configuration. The below options
are supported, in addition to options passed down to the `http_open/3` predicate:

* `k8s_config(Config:dict)` - kubectl configuration - if not provided then the configuation is loaded by first succesfull of the following possibilities

  * resolving and mergin config files specified by environment variable `KUBECONFIG` if existing;
  * loading `~/.kube/config` file if existing;
  * using pod service account if the process is executed from the pod.

* `k8s_context(Context:atom)` - name of the context to use from the `k8s_config` option. If not specified, then
  `current-context` property of `k8s_config` is used as a default value
* `k8s_namespace(Namespace)` - the namespace from which to load resource instance(s). Shall be set to `all` if all namespaces shall be listed
  (will fail if `InstanceName` is bound and resource is namespaced). If not specified then the namespace provided as part of the context or the
  `default` namespace will be used.
* `k8s_resource_types_mode(Mode)` where  `Mode` is one of `cache` (default), `renew`, `remote`, `local`. Default to `local`. Used when resolving
    if the resource is namespaced. See also `k8s_resource_types/2`
* `k8s_selectors(Selectors:list)` - list of selectors to apply for the resource retrieval(plain, not encoded form)
* `k8s_query(Query:term)` - a query parameter to add to the REST call in form as specified by the predicate `uri_query_components/2`. This
  option is concatenated if used multiple times

### `k8s_resources(-ResourceTypes:list(dict), +Options)` is semidet

Unifies the `ResourceTypes` with list of resource types available on the cluster. Primary used for resource
discovery. `Options` are same as for the `k8s_get_resource/6` with the extra option:

* `k8s_resource_types_mode(Mode)` where  `Mode` is one of `cache` (default), `renew`, `remote`, `local`. In the
  standard mode, the resources are cached per cluster and retrieved from the cache if available.
  If `renew` or `remote` mode is requested then the resource list is retrieved directly from the
  API server. The 'remote' and `local` mode leaves the cache untouched. The caching significantly speed up consequent
  operation with kubernetes resources

### `k8s_update_resource(+ApiGroup:atom, +Version:atom, +ResourceTypeName:atom, +Instanc:dict, -InstanceOut:dict, +Options:list)` is semidet

Updates a resource at the Kubernetes API and unifies the `InstanceOut` with the server response. Options are same as for the predicate
`k8s_get_resource/6`

### `k8s_watch_resources(:Callback, +ApiGroup:atom, +Version:atom, +ResourceTypeName:atom, +Options:list)` is det

This predicate watches the changes of the resource list specified by the arguments and call `call(:Goal, ChangeType:atom, ResourceInstance:dict)`
for each modification in the list. The `ChangeType` is one of the `added`, `modified`, `deleted` atom. Initial list of instances
is provided as sequence of `added` callbacks after the call of this predicate.

The call is blocking the caller thread.

`Options` are same as for the `k8s_get_resource/6` with the extra option:

* `k8s_resource_version(ResourceVersion:atom)` - if specified the initial list is retrieved for the changed since the specified resource version.
  This option is used primary for internal purposes, and can be reset back to 0.

### `k8s_watch_resources_async(:Callback, +ApiGroup:atom, +Version:atom, +ResourceTypeName:atom, -StopWatcher, +Options:list)` is det

Forks the separated background thread in which the predicate `k8s_watch_resources/5` is executed.

The background thread can be finished and/or joined by calling `call(StopWatcher, Mode:atom)`,
where `Mode` is either `join`, or `stop`. The later mode will break the watching thread and force it to exit before joining it.
If the `Mode` is unbound, then behavior is equivalent to the `stop` mode, and the `Mode` is boud to the exit status of the watcher thread.

Other arguments are same as for the `k8s_watch_resources/5`.
Be aware that the `Callback` is invoked from the different thread than the thread calling this predicate.

## Development

To debug the module, load the `debug.pl` file into prolog top.

## Testing

Only manual tests of API calls executed
