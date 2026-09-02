--!strict
--!optimize 1

local LINQ, LINQMethods = {}, {}
LINQMethods.__index = LINQMethods

--[=[
	@class LINQ

	A lazy, chainable query interface for Luau arrays and dictionaries.

	Intermediate operations build an immutable execution pipeline. The pipeline
	is evaluated only when a terminal operation such as `ToArray`, `First`, or
	`Count` is called.

	```lua
	local result = LINQ({1, 2, 3, 4})
		:Where(function(value)
			return value % 2 == 0
		end)
		:Select(function(value)
			return value * 10
		end)
		:ToArray()

	-- {20, 40}
	```
]=]

-- | Types | --
export type Predicate<T> =  (value : T) -> boolean

export type Selector<T, U> = (value : T) -> U
export type ManySelector<T, U> = (value : T) -> { U }
export type NumberSelector<T> = (value : T) -> number;

export type Comparer<T> = (a : T, b : T) -> boolean
export type Accumulator<A, T> = (accumulator : A, value : T) -> A

export type Entry <K, V> = { Key : K; Value : V}
type SortEntry = {
	value : any;
	keys : { any };
	index : number
}

export type LINQ<T> = {
	Where : (self : LINQ<T>, predicate : Predicate<T>) -> LINQ<T>;
	Select : <U>(self : LINQ<T>, selector : Selector<T, U>) -> LINQ<U>;

	Skip : (self : LINQ<T>, count : number) -> LINQ<T>;
	Take : (self : LINQ<T>, count : number) -> LINQ<T>;

	ToArray : (self : LINQ<T>) -> { T };
	ToDictionary : <K, V>(self : LINQ<T>, keySelector : Selector<T, K>, valueSelector : Selector<T, V>) -> { [K] : V };

	First : (self : LINQ<T>) -> T;
	FirstOrNil : (self : LINQ<T>) -> T?;

	Any : (self : LINQ<T>) -> boolean;
	Count : (self : LINQ<T>) -> number;

	Distinct : (self : LINQ<T>) -> LINQ<T>;
	SelectMany : <U>(self : LINQ<T>, selector : ManySelector<T, U>) -> LINQ<U>;

	OrderBy : <K>(self : LINQ<T>, keySelector : Selector<T, K>, descending : boolean?, comparer : Comparer<K>?) -> LINQ<T>;
	GroupBy : <K>(self : LINQ<T>, keySelector : Selector<T, K>) -> { [K] : { T } };
	DistinctBy : <K>(self : LINQ<T>, keySelector : Selector<T, K>) -> LINQ<T>;
	ThenBy : <K>(self : LINQ<T>, keySelector : Selector<T, K>, descending : boolean?, comparer : Comparer<K>?) -> LINQ<T>;

	Aggregate : <A>(self : LINQ<T>, seed : A, accumulator : Accumulator<A, T>) -> A;

	Reverse : (self : LINQ<T>) -> LINQ<T>;

	Concat : (self : LINQ<T>, values : { T }) -> LINQ<T>;
	Last : (self : LINQ<T>) -> T;
	LastOrNil : (self : LINQ<T>) -> T?;

	All : (self : LINQ<T>, predicate : Predicate<T>) -> boolean;

	Min: (self : LINQ<T>, comparer : Comparer<T>?) -> T;
	Max : (self : LINQ<T>, comparer : Comparer<T>?) -> T;
	Sum : (self : LINQ<T>, selector : NumberSelector<T>?) -> number;
	Average : (self : LINQ<T>, selector : NumberSelector<T>?) -> number;
}
type LINQImpl = {
	_source : Source;
	_segments : { ExecutionSegment };
	_isEmpty : boolean;

	_enumerate : (self : LINQImpl, emit : (value : any) -> boolean?) -> ();
}

type OrderCriterion = {
	keySelector : Selector<any, any>;
	descending : boolean;
	comparer : Comparer<any>?;
}

type ArraySource = { kind : "array"; values : { any } }
type DictionarySource = { kind : "dictionary"; values : any }

-- A query's backing collection.
-- Dictionary sources enumerate as `Entry<Key, Value>` values.
type Source = ArraySource | DictionarySource

-- Receives one value from the upstream pipeline.
-- Return false to stop enumeration.
type Push = (value : any) -> boolean

-- Enumerates values into `push` until exhausted or `push` returns false.
type Producer = (push : Push) -> ()

--[=[
	Query execution uses a push pipeline.

	A `Producer` reads values from a source and passes each value into a `Push`.
	Streaming segments wrap that Push to transform, filter, or expand values.

	Returning false propagates upstream and stops enumeration, allowing terminal
	operations such as `First` and `Any` to short-circuit.

	Producer -> Push -> Push -> Push -> Terminal
]=]

type FilterSegment = {
	kind : "filter";
	predicates : { Predicate<any> };
}
type MapSegment = {
	kind : "map";
	selector : Selector<any, any>;
}
type WindowSegment = { -- represents any contiguous combination of: Skip() & Take() (e.g. Skip(5):Take(10):Skip(5) ==> skip = 10; take = 5)
	kind : "window";

	skip : number;
	take : number? -- nil = no upper limit
}
type DistinctSegment = { 
	kind : "distinct";
	keySelector : Selector<any, any>?
}
type FlatMapSegment = { 
	kind : "flatMap";
	selector : ManySelector<any, any>;
}
type OrderSegment = {
	kind : "order";
	criteria : { OrderCriterion }
}
type ReverseSegment = {
	kind : "reverse"
}
type ConcatSegment = {
	kind : "concat";
	values : { any }
}

-- Describes one deferred operation in a query pipeline.
--
-- Streaming:
--   filter, map, window, distinct, flatMap
--
-- Structural:
--   concat
--
-- Buffering:
--   order, reverse
type ExecutionSegment = FilterSegment | MapSegment | WindowSegment | DistinctSegment | FlatMapSegment | OrderSegment | ReverseSegment | ConcatSegment -- one immutable step in a query's pipeline; compileStreaming composes these into a single Push chain

-- | Helper Functions | --
const function isDictionary(t)
	if #t == 0 then return true end
	for k, _ in t do
		if type(k) ~= "number" or k % 1 ~= 0 or k < 1 then return true end
	end
	return false
end
const function validateCount(count : number)
	if not (count >= 0 and count < math.huge and math.floor(count) == count) then error(`Failed to {debug.info(2, "n")}: count must be a non-negative integer!`) end
end
const function validateSelector(value : any?)
	if not (value ~= nil) then error(`Failed to {debug.info(2, "n")}: :Select() cannot return nil because arrays cannot store nil values!`) end
end

-- Segment constructors only describe deferred work.
-- They do not execute the query.
const function createFilterSegment(predicates : { Predicate<any> }) : FilterSegment
	return {
		kind = "filter";
		predicates = predicates;
	}
end
const function createMapSegment(selector : Selector<any, any>) : MapSegment
	return {
		kind = "map";
		selector = selector;
	}
end
-- Combines adjacent Skip/Take operations into one sequence window.
const function createWindowSegment(skip : number, take : number?) : WindowSegment
	return {
		kind = "window";
		skip = skip;
		take = take
	}
end
const function createDistinctSegment(keySelector : Selector<any, any>?) : DistinctSegment
	return {
		kind = "distinct";
		keySelector = keySelector or nil
	}
end
const function createFlatMapSegment(manySelector : ManySelector<any, any>) : FlatMapSegment
	return {
		kind = "flatMap";
		selector = manySelector
	}
end
const function createOrderSegment(criteria : { OrderCriterion }) : OrderSegment
	return {
		kind = "order";
		criteria = criteria
	}
end
const function createReverseSegment() : ReverseSegment return { kind = "reverse" } end
const function createConcatSegment(values : { any }) : ConcatSegment
	return {
		kind = "concat";
		values = values
	}
end

-- Pre-sizes ToArray's output buffer for the general execution branch; dictionaries just get 0 (a second pass to count them isn't worth it)
const function getCapacity(source : Source, segments : { ExecutionSegment })
	if source.kind == "dictionary" then return 0 end -- expensive to perform extra pass on dictionaries

	local capacity = #source.values

	for i = 1, #segments do
		local segment = segments[i]

		if segment.kind == "window" then
			capacity = math.max(0, capacity - segment.skip)
			if segment.take ~= nil then capacity = math.min(capacity, segment.take) end
		end
	end

	return capacity
end

-- Fast path for a single fused FilterSegment.
const function filterFast(source : { any }, segment : FilterSegment) : { any }
	local output = table.create(#source)
	local outputNDX = 0

	local predicates = segment.predicates

	for i = 1, #source do
		local value, include = source[i], true

		for predicateNDX = 1, #predicates do
			if not predicates[predicateNDX](value) then include = false break end
		end
		if include == true then
			outputNDX += 1
			output[outputNDX] = value
		end
	end

	return output
end

-- Fast path for a single fused MapSegment.
const function selectFast(source : { any }, segment : MapSegment) : { any }
	local output = table.create(#source)
	local selector = segment.selector

	for i = 1, #source do
		local value = selector(source[i])
		validateSelector(value)

		output[i] = value
	end
	return output
end

-- Fast path for a single WindowSegment.
const function windowFast(source : { any }, segment : WindowSegment) : { any }
	local take, skip = segment.take, segment.skip

	if take == 0 then return {} end

	local sourceLength, firstNDX = #source, skip + 1
	if firstNDX > sourceLength then return {} end

	local lastNDX = sourceLength
	if take ~= nil then lastNDX = math.min(lastNDX, skip + take) end

	if lastNDX < firstNDX then return {} end

	local outputLength, outputNDX = lastNDX - firstNDX + 1, 0
	local output = table.create(outputLength)

	for i = firstNDX, lastNDX do
		outputNDX += 1
		output[outputNDX] = source[i]
	end

	return output
end

-- Fast path for Filter -> Map.
const function filterSelectFast(source : { any }, filterSegment : FilterSegment, mapSegment : MapSegment)
	local output, outputNDX = table.create(#source), 0
	local predicates, selector = filterSegment.predicates, mapSegment.selector

	for i = 1, #source do
		local sourceValue, include = source[i], true
		for predicateNDX = 1, #predicates do
			if not predicates[predicateNDX](sourceValue) then include = false break end
		end

		if include == true then 
			local mappedValue = selector(sourceValue)
			validateSelector(mappedValue)

			outputNDX += 1
			output[outputNDX] = mappedValue
		end
	end

	return output
end

-- Fast path for Map -> Filter.
const function selectFilterFast(source : { any }, mapSegment : MapSegment, filterSegment : FilterSegment)
	local output, outputNDX = table.create(#source), 0
	local selector, predicates = mapSegment.selector, filterSegment.predicates

	for i = 1, #source do
		local sourceValue, include = source[i], true
		local value = selector(sourceValue)

		validateSelector(value)

		for predicateNDX = 1, #predicates do
			if not predicates[predicateNDX](value) then include = false break end
		end

		if include == true then
			outputNDX += 1
			output[outputNDX] = value
		end
	end

	return output
end

-- Fast path for Filter -> Map -> Filter.
const function filterSelectFilterFast(source : { any }, filterSegment1 : FilterSegment, mapSegment : MapSegment, filterSegment2 : FilterSegment)
	local output, outputNDX = table.create(#source), 0
	local predicates1, selector, predicates2 = filterSegment1.predicates, mapSegment.selector, filterSegment2.predicates

	for i = 1, #source do
		local value, include = source[i], true

		for j = 1, #predicates1 do
			if not predicates1[j](value) then include = false break end
		end
		if include == false then continue end

		local mappedValue = selector(value)
		validateSelector(mappedValue)

		for k = 1, #predicates2 do
			if not predicates2[k](mappedValue) then include = false break end
		end

		if include == true then
			outputNDX += 1
			output[outputNDX] = mappedValue
		end
	end

	return output
end

-- Fast path for Filter -> Map -> Window.
const function filterSelectWindowFast(source : { any }, filterSegment : FilterSegment, mapSegment : MapSegment, windowSegment : WindowSegment) : { any }
	local take, skip = windowSegment.take, windowSegment.skip
	if take == 0 then return {} end

	local predicates, selector = filterSegment.predicates, mapSegment.selector

	local capacity = take or math.max(0, #source - skip)
	local output, outputNDX = table.create(capacity), 0

	local seenCount = 0

	for i = 1, #source do
		local value, include = source[i], true

		for j = 1, #predicates do
			if not predicates[j](value) then include = false break end
		end
		if include == false then continue end

		local mappedValue = selector(value)
		validateSelector(mappedValue)

		seenCount += 1

		if seenCount <= skip then continue end

		outputNDX += 1
		output[outputNDX] = mappedValue

		if take ~= nil and outputNDX >= take then break end
	end

	return output
end

-- Fast path for Map -> Filter -> Window.
const function selectFilterWindowFast(source : { any }, mapSegment : MapSegment, filterSegment : FilterSegment, windowSegment : WindowSegment) : { any }
	local take, skip = windowSegment.take, windowSegment.skip
	if take == 0 then return {} end

	local selector, predicates = mapSegment.selector, filterSegment.predicates

	local capacity = take or math.max(0, #source - skip)
	local output, outputNDX = table.create(capacity), 0

	local seenCount = 0

	for i = 1, #source do
		local value, include = source[i], true
		value = selector(value)
		validateSelector(value)

		for j = 1, #predicates do
			if not predicates[j](value) then include = false break end
		end
		if include == false then continue end

		seenCount += 1
		if seenCount <= skip then continue end

		outputNDX += 1
		output[outputNDX] = value

		if take ~= nil and outputNDX >= take then break end
	end

	return output
end

-- Fast path for Filter -> Distinct -> Map.
const function filterDistinctSelectFast(source : { any }, filterSegment : FilterSegment, distinctSegment : DistinctSegment, mapSegment : MapSegment)
	local output, outputNDX = table.create(#source), 0
	local predicates, keySelector, selector = filterSegment.predicates, distinctSegment.keySelector, mapSegment.selector
	local seen : { [any] : boolean } = {}

	for i = 1, #source do
		local value, include = source[i], true

		for j = 1, #predicates do
			if not predicates[j](value) then include = false break end
		end
		if include == false then continue end

		local key = value

		if keySelector ~= nil then
			key = keySelector(value)
			validateSelector(key)
		end
		if seen[key] == true then continue end

		seen[key] = true

		local mappedValue = selector(value)
		validateSelector(mappedValue)

		outputNDX += 1
		output[outputNDX] = mappedValue
	end

	return output
end

--[=[
	@private
	@within LINQ

	Compiles a range of streaming segments into one Push chain.

	Segments are composed from right to left so each segment forwards accepted
	values to the next Push. No intermediate result tables are allocated.

	Returning false from any downstream Push stops upstream enumeration.

	Buffering and structural segments are handled by `executeSegments`.
]=]
const function compileStreaming(segments : { ExecutionSegment }, firstNDX : number, lastNDX : number, emit : Push) : Push
	local push = emit

	for i = lastNDX, firstNDX, -1 do
		local segment, nextPush = segments[i], push

		if segment.kind == "filter" then
			local predicates = segment.predicates

			push = function(value : any) : boolean
				for predicateNDX = 1, #predicates do
					if not predicates[predicateNDX](value) then return true end -- reject value but continue source enumerations
				end

				return nextPush(value)
			end
		elseif segment.kind == "map" then
			local selector = segment.selector

			push = function(value : any) : boolean
				local mappedValue = selector(value)
				validateSelector(mappedValue)

				return nextPush(mappedValue)
			end
		elseif segment.kind == "window" then
			local skip, take = segment.skip, segment.take
			local seenCount = 0

			push = function(value : any) : boolean
				seenCount += 1

				if seenCount <= skip then return true end
				if take == nil then return nextPush(value) end
				if take == 0 then return false end

				local taken = seenCount - skip
				if not nextPush(value) then return false end

				return taken < take
			end
		elseif segment.kind == "distinct" then
			local seen : { [any] : boolean } = {}
			local keySelector = segment.keySelector

			push = function(value : any) : boolean
				local key = value

				if keySelector ~= nil then
					key = keySelector(value)
					validateSelector(key)
				end

				if seen[key] then return true end

				seen[key] = true
				return nextPush(value)
			end
		elseif segment.kind == "flatMap" then
			local selector = segment.selector

			push = function(value : any) : boolean
				local collection = selector(value)

				for childNDX = 1, #collection do
					local child = collection[childNDX]
					validateSelector(child)

					if not nextPush(child) then return false end
				end
				return true
			end
		elseif segment.kind == "order" then error("Order segments cannot be compiled as streaming segments!") end
	end

	return push
end

--[=[
	@private
	@within LINQ

	Enumerates a Source into `push`.

	Array values are emitted directly. Dictionary entries are emitted as
	`Entry<Key, Value>` values. Enumeration stops when `push` returns false.
]=]
const function produceSource(source : Source, push : Push)
	if source.kind == "array" then
		local values = source.values
		for i = 1, #values do
			if not push(values[i]) then return end
		end
	else
		for key, value in source.values do
			local entry = {
				Key = key;
				Value = value
			}

			if not push(entry) then return end
		end
	end
end

--[=[
	@private
	@within LINQ

	Sorts a materialized sequence using an OrderSegment.

	Sort keys are computed once per value. Multiple criteria support `ThenBy`,
	and the original index preserves ordering when all criteria compare equal.
]=]
const function sortBuffer(buffer : { any }, segment : OrderSegment)
	local criteria = segment.criteria
	local decorated : { SortEntry } = table.create(#buffer) :: { SortEntry }

	for i = 1, #buffer do
		local value = buffer[i]
		local keys = table.create(#criteria)

		for criterionNDX = 1, #criteria do
			local key = criteria[criterionNDX].keySelector(value)

			validateSelector(key)
			keys[criterionNDX] = key
		end
		decorated[i] = {
			value = value;
			keys = keys;
			index = i
		}
	end

	local defaultComparer = function(a : any, b : any) return a < b end
	table.sort(decorated, function(a : SortEntry, b : SortEntry)
		for i = 1, #criteria do
			local criterion = criteria[i]
			local lessThan = criterion.comparer or defaultComparer

			local aKey, bKey = a.keys[i], b.keys[i]
			local aBeforeB, bBeforeA = lessThan(aKey, bKey), lessThan(bKey, aKey)

			if aBeforeB ~= bBeforeA then
				if criterion.descending == true then return bBeforeA end
				return aBeforeB
			end
		end

		return a.index < b.index -- prioritize original sequential order
	end)

	for i = 1, #decorated do buffer[i] = decorated[i].value end
end

--[=[
	@private
	@within LINQ

	Executes a query pipeline from `startNDX`.

	Streaming segments are compiled directly into the current Producer.

	Structural barriers:
	- `concat` wraps the Producer and appends another sequence without buffering.

	Buffering barriers:
	- `order` materializes and sorts upstream results.
	- `reverse` materializes and reverses upstream results.

	After a barrier is processed, execution resumes using the resulting Producer.
]=]
const function executeSegments(producer : Producer, segments : { ExecutionSegment }, startNDX : number, emit : Push)
	local barrierNDX : number?
	for i = startNDX, #segments do
		if segments[i].kind == "order" or segments[i].kind == "reverse" or segments[i].kind == "concat" then barrierNDX = i break end
	end

	if barrierNDX == nil then
		local push = compileStreaming(segments, startNDX, #segments, emit)
		return producer(push)
	end

	-- Collect everything before the buffering barrier

	local barrier = segments[barrierNDX]
	local execProducer : Producer

	if barrier.kind == "concat" then
		local upstreamProducer : Producer
		if barrierNDX > startNDX then
			upstreamProducer = function(push : Push)
				local compiledPush = compileStreaming(segments, startNDX, barrierNDX - 1, push)
				producer(compiledPush)
			end
		else upstreamProducer = producer end

		local values = barrier.values

		execProducer = function(push : Push)
			local continue = true
			upstreamProducer(function(value : any) continue = push(value) return continue end)

			if continue == false then return end

			for i = 1, #values do
				if not push(values[i]) then return end
			end
		end
	else
		local buffer, bufferNDX = {}, 0
		local collect = function(value : any) : boolean
			bufferNDX += 1
			buffer[bufferNDX] = value

			return true
		end

		if barrierNDX > startNDX then
			local push = compileStreaming(segments, startNDX, barrierNDX - 1, collect)
			producer(push)
		else producer(collect) end

		if barrier.kind == "order" then
			sortBuffer(buffer, barrier)
		else
			local left, right = 1, #buffer
			while left < right do
				buffer[left], buffer[right] = buffer[right], buffer[left]
				left += 1
				right -= 1
			end
		end

		execProducer = function(push : Push)
			for i = 1, #buffer do
				if not push(buffer[i]) then return end
			end
		end
	end

	executeSegments(execProducer, segments, barrierNDX + 1, emit)
end

--[=[
	@private
	@within LINQ

	Creates a query from an existing Source and deferred segment list.
]=]
const function newLINQ<T>(source : Source, segments : { ExecutionSegment }?, isEmpty : boolean?) : LINQ<T>
	return setmetatable({
		_source = source;
		_segments = segments or {};
		_isEmpty = isEmpty or false
	}, LINQMethods) :: LINQ<T>
end

--[=[
	@within LINQ

	Creates a query over an array.

	The source table is not copied.

	@param source {T}
	@return LINQ<T>
]=]
function LINQ.from<T>(source : { T }) : LINQ<T>
	local source = {
		kind = "array";
		values = source
	} :: ArraySource

	return newLINQ(source)
end

--[=[
	@within LINQ

	Creates a query over a dictionary.

	Dictionary entries are enumerated as `{ Key, Value }`.

	@param source {[K]: V}
	@return LINQ<Entry<K, V>>
]=]
function LINQ.fromDictionary<K, V>(source : { [K] : V }) : LINQ<Entry<K, V>>
	local dictionarySource : DictionarySource = {
		kind = "dictionary";
		values = source
	}

	return newLINQ(dictionarySource) :: LINQ<Entry<K, V>>
end

--[=[
	@private
	@within LINQ

	Executes the query and emits each resulting value.

	Used by terminal operations that do not use a specialized fast path.
]=]
function LINQMethods._enumerate(self : LINQImpl, emit : (value : any) -> boolean?)
	if self._isEmpty == true then return end

	local function producer(push : Push)
		produceSource(self._source, push)
	end

	executeSegments(producer, self._segments, 1, function(value : any) return emit(value) ~= false end)
end

-----------------------------------------------------------------------------------------------
------------------------------------INTERMEDIATE OPERATIONS------------------------------------
-----------------------------------------------------------------------------------------------

--[=[
	@within LINQ

	Returns a query containing values that satisfy `predicate`.

	Adjacent `Where` operations are fused into one filter segment.
]=]
function LINQMethods.Where<T>(self : LINQImpl & LINQ<T>, predicate : Predicate<T>) : LINQ<T>
	if self._isEmpty == true then return self end

	local segments = table.clone(self._segments)
	local last = segments[#segments]

	if last ~= nil and last.kind == "filter" then -- combine where calls
		local predicates = table.clone(last.predicates) :: { Predicate<any> }
		table.insert(predicates, predicate)

		segments[#segments] = createFilterSegment(predicates)
	else
		table.insert(segments, createFilterSegment({predicate}))
	end

	return newLINQ(self._source, segments, false)
end

--[=[
	@within LINQ

	Projects each value through `selector`.

	Adjacent `Select` operations are composed into one map segment.
	The selector must not return nil.
]=]
function LINQMethods.Select<T, U>(self : LINQImpl & LINQ<T>, selector : Selector<T, U>) : LINQ<U>
	if self._isEmpty == true then return self end

	local segments = table.clone(self._segments)
	local last = segments[#segments]

	if last ~= nil and last.kind == "map" then -- combine select calls
		local prevSelector = last.selector
		segments[#segments] = createMapSegment(function(value : any) : any
			local intermediate = prevSelector(value)
			validateSelector(intermediate)

			return selector(intermediate)
		end)
	else
		table.insert(segments, createMapSegment(selector))
	end

	return newLINQ(self._source, segments, false)
end

--[=[
	@within LINQ

	Projects each value to an array and flattens the arrays into one sequence.
]=]
function LINQMethods.SelectMany<T, U>(self : LINQImpl & LINQ<T>, selector : ManySelector<T, U>) : LINQ<U>
	if self._isEmpty == true then return self end

	local segments = table.clone(self._segments)

	table.insert(segments, createFlatMapSegment(selector))
	return newLINQ(self._source, segments, false)
end

--[=[
	@within LINQ

	Skips the first `count` values.

	Adjacent Skip/Take operations are combined into one window.
]=]
function LINQMethods.Skip<T>(self : LINQImpl & LINQ<T>, count : number) : LINQ<T>
	validateCount(count)
	if count == 0 or self._isEmpty == true then return self end

	local segments = table.clone(self._segments)
	local last = segments[#segments]

	if last ~= nil and last.kind == "window" then
		local skip, take = last.skip, last.take

		if take == nil then -- Skip(5):Skip(3) => Skip(8)
			skip += count
		else
			local removed = math.min(count, take) -- Take(10):Skip(5) => window { skip = 5; take = 5}

			skip += removed
			take -= removed
		end

		segments[#segments] = createWindowSegment(skip, take)

		return newLINQ(self._source, segments, false)
	end

	table.insert(segments, createWindowSegment(count))

	return newLINQ(self._source, segments, false)
end

--[=[
	@within LINQ

	Returns at most the first `count` values.

	Enumeration stops once the requested number of values is produced.
]=]
function LINQMethods.Take<T>(self : LINQImpl & LINQ<T>, count : number) : LINQ<T>
	validateCount(count)
	if self._isEmpty == true then return self end

	local segments = table.clone(self._segments)
	local last = segments[#segments]

	if last ~= nil and last.kind == "window" then
		local take = last.take

		if take == nil then 
			take = count
		else take = math.min(take, count) end

		segments[#segments] = createWindowSegment(last.skip, take)

		return newLINQ(self._source, segments, take == 0)
	end

	table.insert(segments, createWindowSegment(0, count))

	return newLINQ(self._source, segments, count == 0)
end

--[=[
	@within LINQ

	Returns unique values, preserving the first occurrence of each value.

	Tables and Instances are compared by identity.
]=]
function LINQMethods.Distinct<T>(self : LINQImpl & LINQ<T>) : LINQ<T>
	if self._isEmpty == true then return self end

	local segments = table.clone(self._segments)
	local last = segments[#segments]

	if last ~= nil and last.kind == "distinct" then return self end -- avoid Distinct():Distinct()

	table.insert(segments, createDistinctSegment())

	return newLINQ(self._source, segments, false)
end

--[=[
	@within LINQ

	Returns the first value for each unique key produced by `keySelector`.
]=]
function LINQMethods.DistinctBy<T, K>(self : LINQImpl & LINQ<T>, keySelector : Selector<T, K>) : LINQ<T>
	if self._isEmpty == true then return self end

	local segments = table.clone(self._segments)

	table.insert(segments, createDistinctSegment(keySelector))
	return newLINQ(self._source, segments, false)
end

--[=[
	@within LINQ

	Appends `values` to the current sequence.

	Concat remains lazy and does not materialize either sequence.
]=]
function LINQMethods.Concat<T>(self : LINQImpl & LINQ<T>, values : { T }) : LINQ<T>
	if #values == 0 then return self end

	local segments = table.clone(self._segments)

	table.insert(segments, createConcatSegment(values))
	return newLINQ(self._source, segments, false)
end

--[=[
	@within LINQ

	Orders values by the key produced by `keySelector`.

	Ordering is stable. This operation buffers all upstream results.

	@param keySelector (T) -> K
	@param descending boolean? -- Defaults to false
	@param comparer ((K, K) -> boolean)? -- Defaults to `<`
]=]
function LINQMethods.OrderBy<T, K>(self : LINQImpl & LINQ<T>, keySelector : Selector<T, K>, descending : boolean?, comparer : Comparer<K>?) : LINQ<T>
	if self._isEmpty == true then return self end

	local segments = table.clone(self._segments)

	table.insert(segments, createOrderSegment({{
		keySelector = keySelector;
		descending = descending == true;
		comparer = comparer or nil
	}}))
	return newLINQ(self._source, segments, false)
end

--[=[
	@within LINQ

	Adds a secondary ordering criterion to the preceding `OrderBy`.

	Must immediately follow `OrderBy` or another `ThenBy`.
]=]
function LINQMethods.ThenBy<T, K>(self : LINQImpl & LINQ<T>, keySelector : Selector<T, K>, descending : boolean?, comparer : Comparer<K>?) : LINQ<T>
	if self._isEmpty == true then return self end

	local segments = table.clone(self._segments)
	local last = segments[#segments]

	assert(last ~= nil and last.kind == "order", "ThenBy must follow OrderBy or ThenBy in the LINQ chain.")

	local criteria = table.clone(last.criteria)
	table.insert(criteria, {
		keySelector = keySelector;
		descending = descending == true;
		comparer = comparer
	})

	segments[#segments] = {
		kind = "order";
		criteria = criteria
	}

	return newLINQ(self._source, segments, self._isEmpty)
end

--[=[
	@within LINQ

	Reverses the sequence.

	This operation buffers all upstream results.
]=]
function LINQMethods.Reverse<T>(self : LINQImpl & LINQ<T>) : LINQ<T>
	if self._isEmpty == true then return self end

	local segments = table.clone(self._segments)
	local last = segments[#segments]

	if last ~= nil and last.kind == "reverse" then -- Reverse():Reverse() cancels out
		table.remove(segments)
	else table.insert(segments, createReverseSegment()) end

	return newLINQ(self._source, segments, false)
end

-----------------------------------------------------------------------------------------------
--------------------------------------TERMINAL OPERATIONS--------------------------------------
-----------------------------------------------------------------------------------------------

--[=[
	@within LINQ

	Executes the query and returns its values as an array.
]=]
function LINQMethods.ToArray<T>(self : LINQImpl & LINQ<T>) : { T } | { Entry<any, any> }
	if self._isEmpty == true then return {} end

	local source, segments = self._source, table.clone(self._segments)

	if #segments == 0 then -- no operations; return clone branch
		if source.kind == "array" then return table.clone(source.values) end

		local output = {}
		for key, value in source.values do
			table.insert(output, {
				Key = key;
				Value = value
			})
		end
		return output
	end

	if source.kind == "array" then -- array specific fast path branches
		local values = source.values

		if #segments == 1 then
			local segment = segments[1]

			if segment.kind == "filter" then
				return filterFast(values, segment)
			elseif segment.kind == "map" then
				return selectFast(values, segment)
			elseif segment.kind == "window" then
				return windowFast(values, segment)
			end
		elseif #segments == 2 then
			local first, second = segments[1], segments[2]

			if first.kind == "filter" and second.kind == "map" then
				return filterSelectFast(values, first, second)
			elseif first.kind == "map" and second.kind == "filter" then
				return selectFilterFast(values, first, second)
			end
		elseif #segments == 3 then
			local first, second, third = segments[1], segments[2], segments[3]

			if first.kind == "filter" and second.kind == "map" and third.kind == "filter" then
				return filterSelectFilterFast(values, first, second, third)
			elseif first.kind == "filter" and second.kind == "map" and third.kind == "window" then
				return filterSelectWindowFast(values, first, second, third)
			elseif first.kind == "filter" and second.kind == "distinct" and third.kind == "map" then
				return filterDistinctSelectFast(values, first, second, third)
			elseif first.kind == "map" and second.kind == "filter" and third.kind == "window" then
				return selectFilterWindowFast(values, first, second, third)
			end
		end
	end

	-- general execution branch
	local capacity = getCapacity(source, segments)
	local output, outputNDX = table.create(capacity), 0

	self:_enumerate(function(value : any) : boolean?
		outputNDX += 1
		output[outputNDX] = value

		return true
	end)

	return output
end

--[=[
	@within LINQ

	Executes the query and creates a dictionary from the selected keys and values.

	Errors if a selector returns nil or multiple values produce the same key.
]=]
function LINQMethods.ToDictionary<T, K , V>(self : LINQImpl & LINQ<T>, keySelector : Selector<T, K>, valueSelector : Selector<T, V>) : { [K] : V }
	if self._isEmpty == true then return {} end

	local output : { [K] : V } = {}

	self:_enumerate(function(value : any) : boolean?
		local key = keySelector(value)
		local mappedValue = valueSelector(value)

		validateSelector(key)
		validateSelector(mappedValue)
		assert(output[key] == nil, "ToDictionary produced a duplicate key!")

		output[key] = mappedValue

		return true
	end)

	return output
end

--[=[
	@within LINQ

	Returns the first value, or nil if the sequence is empty.

	Stops enumeration after the first result.
]=]
function LINQMethods.FirstOrNil<T>(self : LINQImpl & LINQ<T>) : T? | Entry<any, any>?
	if self._isEmpty == true then return nil end

	if #self._segments == 0 then -- no operations
		local source = self._source
		if source.kind == "array" then return source.values[1] end

		local key, value = next(source.values)
		if key == nil then return nil end

		return {
			Key = key;
			Value = value
		}
	end

	local result = nil

	self:_enumerate(function(value)
		result = value
		return false
	end)

	return result
end

--[=[
	@within LINQ

	Returns the first value.

	Errors if the sequence is empty.
]=]
function LINQMethods.First<T>(self : LINQImpl & LINQ<T>) : T | Entry<any, any>
	local result = LINQMethods.FirstOrNil(self)

	if result == nil then error("Sequence contains no elements!", 2) end
	return result
end

--[=[
	@within LINQ

	Returns the last value, or nil if the sequence is empty.
]=]
function LINQMethods.LastOrNil<T>(self : LINQImpl & LINQ<T>) : T?
	if self._isEmpty == true then return nil end

	if #self._segments == 0 and self._source.kind == "array" then return self._source.values[#self._source.values] end -- array optimization

	local result
	self:_enumerate(function(value)
		result = value
		return true
	end)

	return result
end

--[=[
	@within LINQ

	Returns the last value.

	Errors if the sequence is empty.
]=]
function LINQMethods.Last<T>(self : LINQImpl & LINQ<T>) : T
	local result = LINQMethods.LastOrNil(self)

	if result == nil then error("Sequence contains no elements!", 2) end
	return result
end

--[=[
	@within LINQ

	Returns whether every value satisfies `predicate`.

	Stops enumeration after the first failure.
]=]
function LINQMethods.Any<T>(self : LINQImpl & LINQ<T>) : boolean
	if self._isEmpty == true then return false end

	if #self._segments == 0 then -- no operations
		local source = self._source

		if source.kind == "array" then return #source.values > 0 end

		return next(source.values) ~= nil
	end

	local isFound = false

	self:_enumerate(function()
		isFound = true
		return false
	end)

	return isFound
end

--[=[
	@within LINQ

	Returns the number of values produced by the query.
]=]
function LINQMethods.Count<T>(self : LINQImpl & LINQ<T>) : number
	if self._isEmpty == true then return 0 end

	if #self._segments == 0 then
		local source = self._source

		if source.kind == "array" then return #source.values end

		local count = 0
		for _ in source.values do count += 1 end

		return count
	end

	local count = 0

	self:_enumerate(function(value : any)
		count += 1
		return true
	end)

	return count
end

--[=[
	@within LINQ

	Returns the minimum value.

	Uses `<` unless `comparer` is provided. Errors if the sequence is empty.
]=]
function LINQMethods.Min<T>(self : LINQImpl & LINQ<T>, comparer : Comparer<T>?) : T
	local result, isFound : boolean
	local lessThan = comparer or function(a : any, b : any) return a < b end

	self:_enumerate(function(value)
		if not isFound or lessThan(value, result) then
			result = value
			isFound = true
		end
		return true
	end)

	if isFound == false then error("Sequence contains no elements!", 2) end
	return result
end

--[=[
	@within LINQ

	Returns the maximum value.

	Uses `<` unless `comparer` is provided. Errors if the sequence is empty.
]=]
function LINQMethods.Max<T>(self : LINQImpl & LINQ<T>, comparer : Comparer<T>?) : T
	local result, isFound : boolean
	local lessThan = comparer or function(a : any, b : any) return a < b end

	self:_enumerate(function(value : any)
		if not isFound or lessThan(result, value) then
			result = value
			isFound = true
		end
		return true
	end)

	if isFound == false then error("Sequence contains no elements!", 2) end
	return result
end

--[=[
	@within LINQ

	Returns the sum of the sequence.

	If `selector` is provided, its numeric result is summed instead.
]=]
function LINQMethods.Sum<T>(self : LINQImpl & LINQ<T>, selector : NumberSelector<T>?) : number
	local sum = 0

	self:_enumerate(function(value : any)
		local numberValue = selector ~= nil and selector(value) or value
		assert(type(numberValue) == "number", "Sum requires numeric values!")

		sum += numberValue
		return true
	end)

	return sum
end

--[=[
	@within LINQ

	Returns the arithmetic mean of the sequence.

	If `selector` is provided, its numeric result is averaged instead.
	Errors if the sequence is empty.
]=]
function LINQMethods.Average<T>(self : LINQImpl & LINQ<T>, selector : NumberSelector<T>?) : number
	local avg, count = 0, 0

	self:_enumerate(function(value : any)
		local numberValue = selector ~= nil and selector(value) or value
		assert(type(numberValue) == "number", "Average requires numeric values!")

		avg += numberValue
		count += 1

		return true
	end)

	if count == 0 then error("Sequence contains no elements!", 2) end

	return avg / count
end

function LINQMethods.All<T>(self : LINQImpl & LINQ<T>, predicate : Predicate<T>) : boolean
	local result = true

	self:_enumerate(function(value)
		if not predicate(value) then result = false return false end
		return true
	end)

	return result
end

--[=[
	@within LINQ

	Groups values by the key produced by `keySelector`.

	Returns a dictionary whose values are arrays containing each group.
]=]
function LINQMethods.GroupBy<T, K>(self : LINQImpl & LINQ<T>, keySelector : Selector<T, K>) : { [K] : { T } }
	local groups : { [K] : { T } } = {}
	if self._isEmpty == true then return groups end

	self:_enumerate(function(value : T)
		local key = keySelector(value)
		validateSelector(key)

		local group = groups[key]
		if group == nil then
			group = {}
			groups[key] = group
		end

		group[#group + 1] = value
		return true
	end)

	return groups
end

--[=[
	@within LINQ

	Reduces the sequence to one value using `accumulator`, beginning with `seed`.

	@param seed A
	@param accumulator (A, T) -> A
	@return A
]=]
function LINQMethods.Aggregate<T, A>(self : LINQImpl & LINQ<T>, seed : A, accumulator : Accumulator<A, T>)
	local result = seed

	self:_enumerate(function(value : T)
		result = accumulator(result, value)
		return true
	end)

	return result
end

table.freeze(LINQMethods)

local mt = {  __call = function(self, source) if isDictionary(source) == true then return LINQ.fromDictionary(source) else return LINQ.from(source) end end }
setmetatable(LINQ, mt)

return table.freeze(LINQ)