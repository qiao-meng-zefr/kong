local cache_prewarm = {}


local ENTITIES_TO_PREWARM = {
  "services",
  "plugins",
}


local function cache_prewarm_single_entity(entity_name)
  local dao = kong.db[entity_name]
  if not dao then
    return nil, "Invalid entity name found when prewarming the cache: " .. tostring(entity_name)
  end

  for entity, err in dao:each(1000) do
    if err then
      return nil, err
    end

    local cache_key = dao:cache_key(entity)
    local ok, err   = kong.cache:get(cache_key, nil, function()
      return entity
    end)
    if not ok then
      return nil, err
    end
  end

  return true
end


-- Loads entities from the database into the cache, for rapid subsequent
-- access. This function is intented to be used during worker initialization
-- The list of entities to be loaded is defined by the ENTITIES_TO_PREWARM
-- variable.
function cache_prewarm.execute()
  -- kong.db and kong.cache might not be active while running tests
  if not kong.db or not kong.cache then
    return true
  end

  for _, entity_name in ipairs(ENTITIES_TO_PREWARM) do
    local ok, err = cache_prewarm_single_entity(entity_name)
    if not ok then
      return nil, err
    end
  end

  return true
end


return cache_prewarm
