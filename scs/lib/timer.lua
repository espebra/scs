local M = {}

local common = require "common"
local ngx = ngx

function M.initiate_periodic_health_checks(delay)
    local cache_key = "periodic_health_checks_started"
    local cache = ngx.shared.cache
    if cache then
        local value, flags = cache:get(cache_key)
        if value then
            -- Periodic health checks are already running
            return
        end
    end

    local succ, err, forcible = cache:set(cache_key, true)
    if succ then
        ngx.log(ngx.INFO, "Periodic health checks started")
    else
        ngx.log(ngx.ERR, "Unable to mark periodic health checks as started")
    end

    local handler
    handler = function (premature)
        -- do some routine job in Lua just like a cron job
        if premature then
            return
        end

        local hosts = common.get_hosts()
        while true do
            common.update_status(hosts)
            ngx.sleep(delay)
        end
        return
    end

    local ok, err = ngx.timer.at(0, handler)
    if not ok then
        ngx.log(ngx.ERR, "Failed to create the health check timer: " .. err)
        return
    end
end

--function M.initiate_batch_synchronization(delay)
--    if ngx.shared.synchronization_timer then
--        return
--    else
--        ngx.shared.synchronization_timer = true
--    end
--
--    local handler
--    handler = function (premature)
--        -- do some routine job in Lua just like a cron job
--        if premature then
--            return
--        end
--
--        local path = common.get_storage_directory()
--        if not common.is_file(path) then
--            ngx.log(ngx.ERR,"Unable to start the sync prosess!" .. path .. " does not exist")
--            return
--        end
--
--        while true do
--            local start = ngx.time()
--            local versions = {}
--
--            ngx.log(ngx.ERR,"Batch job started")
--            common.full_replication()
--            local elapsed = ngx.now() - start
--            ngx.log(ngx.ERR,"Batch job completed in " .. elapsed .. " seconds.")
--            ngx.sleep(delay)
--        end
--    end
--
--    local ok, err = ngx.timer.at(5, handler)
--    if not ok then
--        ngx.log(ngx.ERR, "Failed to create the synchronization timer: " .. err)
--        return
--    end
--end

return M
