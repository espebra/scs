local M = {}

local common = require "common"
local cache = require "cache"
local Configuration = require "configuration"
local ngx = ngx

function M.initiate_periodic_health_checks(delay)
    local key = "periodic_health_checks_started"
    local status = cache.get_cache(key)
    if status then
        -- Timers have already been started
        return
    end
    cache.set_cache(key, true)

    local conf = Configuration()

    local handler
    handler = function (premature)
        -- do some routine job in Lua just like a cron job
        if premature then
            return
        end

        local hosts = conf.hosts
        while true do

            for host, h in pairs(conf.hosts) do
                ngx.log(ngx.DEBUG,"Health checking host " .. host .. " port " .. h['port'])
                common.update_host_status(host,h['port'])

                -- Some minor sleep between each check
                ngx.sleep(0.1)
            end
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
