# LINQ

A typed, lazy, chainable query API for Luau collections.

LINQ provides a familiar way to filter, transform, group, sort, and aggregate arrays and dictionaries without repeatedly writing traversal logic.

```lua
local LINQ = require(Packages.LINQ)

local result = LINQ(workspace:GetDescendants())
	:Where(function(instance)
		return instance:IsA("BasePart")
	end)
	:FirstOrNil()
```

## Features

* Lazy query execution
* Chainable, strongly typed API
* Array and dictionary support
* Short-circuiting terminals such as `First`, `Any`, and `All`
* Streaming execution for most operators
* Fused fast paths for common query shapes
* Stable `OrderBy` / `ThenBy`
* `Distinct` and `DistinctBy`
* `SelectMany`
* `GroupBy`
* `Aggregate`, `Sum`, `Average`, `Min`, and `Max`

## Installation

```toml
[dependencies]
LINQ = "pineappleblaster/linq-lua@0.3.0"
```

Then require it from your Wally packages directory:

```lua
local LINQ = require(Packages.LINQ)
```

## Usage

### Filtering

```lua
local enemies = LINQ(characters)
	:Where(function(character)
		return character:GetAttribute("Team") == "Enemy"
	end)
	:ToArray()
```

### Mapping

```lua
local names = LINQ(players)
	:Select(function(player)
		return player.Name
	end)
	:ToArray()
```

### Finding a value

```lua
local part = LINQ(workspace:GetDescendants())
	:Where(function(instance)
		return instance:IsA("BasePart")
	end)
	:FirstOrNil()
```

### Sorting

```lua
local sorted = LINQ(players)
	:OrderBy(function(player)
		return player.Level
	end, true)
	:ThenBy(function(player)
		return player.Name
	end)
	:ToArray()
```

### Dictionaries

Dictionary sources are exposed as entries containing `Key` and `Value`.

```lua
local entries = LINQ({
	Health = 100,
	Speed = 16,
	Damage = 25,
})
	:Where(function(entry)
		return entry.Value >= 25
	end)
	:ToArray()
```

## API

### Query operators

```text
Where
Select
Skip
Take
Distinct
DistinctBy
SelectMany
OrderBy
ThenBy
Reverse
Concat
```

### Terminal operators

```text
ToArray
ToDictionary
First
FirstOrNil
Last
LastOrNil
Any
All
Count
GroupBy
Aggregate
Min
Max
Sum
Average
```

## Execution Model

Queries are deferred until a terminal operation is called.

Most operators are compiled into a streaming pipeline:

```text
Source → Where → Select → Take → Terminal
```

Values move through the query one at a time without allocating an intermediate table for each stage.

Operations such as `OrderBy` and `Reverse` require the complete upstream sequence and therefore act as buffering barriers:

```text
Source → Where → [OrderBy] → Select → Take
```

Only the required portion of the pipeline is materialized; execution continues streaming afterward.

The pipeline also supports upstream cancellation, allowing operations such as `First`, `Any`, and `Take` to stop enumeration as soon as their result is known.

## Notes

Query objects retain a reference to their original source table rather than copying it. Mutating the source before execution can therefore affect the query result.

Callbacks should not return `nil` where the resulting value must be stored in an array.

## License

MIT
