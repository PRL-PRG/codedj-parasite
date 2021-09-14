# CodeDJ - Parasite v4

Parasite is responsible for the implementation & updates to the append only datastore of software repositories. This is the fourth version, which hopefully will be final, for some time...

## TL;DR: Major changes from version 3

- complete overhaul of internal representation (append only tables and various indexers as opposed to stores, linked stores, split stores, etc.)
- substores are independent datastores that just happen to share project ids across themselves, but each substore has its own savepoints, etc. 
- much more resilient and robust
- minimalist to the bone

## Datastore



## Updater


## TODO

- nested datastores, how to deal with savepoints?  
