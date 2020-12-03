# Process types

## GitlabSync   
- source
- webhook
- use genstage ? no, because we can only fetch a fixed amount of issues (the
  existing ones), and the webhook is purely push based. Also it is shared.

## DataStore.ETS<Issue>
- source
- listen to GitlabSync
- use genstage ? No, it is shared. But it can emit streams of query results for
  producers to consume according to demand.

## Scheduler<Report<Issue>>
- source
- use genstage ? No, it may not be shared, but it is purely push based.

## AlertGenerator<Issue>
- depend on DataStore for events. (Or GitlabSync ? No, it should depend on the
  datastore so we do not refetch if it crashes, as we already have the data
  locally).
- The datastore can send a deferred stream of events, so the alert generator
  will actually directly pull the data items from ETS.
- use genstage ? Yes, all events are independent but does not require tracking,
  so they can be handled in batches.

## Delayer<Alert<Issue>>
- source : emit delayed events after timeouts
- sink : receive events to delay/clear

## Broadcast<?>
- direct broadcast or relay to a queue, so it is a sync or a simple operation