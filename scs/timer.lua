local M = {}

local common = require "scs.common"
local ngx = ngx

function M.initiate_periodic_health_checks(delay)
    if ngx.shared.health_check_timer then
        return
    else
        ngx.shared.health_check_timer = true
    end

    local handler
    handler = function (premature)
        -- do some routine job in Lua just like a cron job
        if premature then
            return
        end
        local ok, err = ngx.timer.at(delay, handler)
        if ok then
            local port = common.get_bind_port()
            local sites = common.get_all_sites()
            sites = common.randomize_table(sites)
            for i,site in ipairs(sites) do
                --ngx.log(ngx.ERR,"Checking site " .. site)
                local hosts = common.get_site_hosts(site)
                hosts = common.randomize_table(hosts)
                for i,host in ipairs(hosts) do
                    --ngx.log(ngx.ERR,"Checking host " .. host .. " on site " .. site)
                    local status=common.remote_host_availability(host, port)
                    -- Status is true or false to indicate if the host is
                    -- available or not.
                    common.update_host_status(host,status)
                end
            end
        else
            ngx.log(ngx.ERR, "Failed to create the timer: ", err)
            return
        end
    end

    local ok, err = ngx.timer.at(delay, handler)
    if not ok then
        ngx.log(ngx.ERR, "failed to create the timer: ", err)
        return
    end
end

function M.initiate_batch_synchronization(delay)
    if ngx.shared.synchronization_timer then
        return
    else
        ngx.shared.synchronization_timer = true
    end

    local handler
    handler = function (premature)
        -- do some routine job in Lua just like a cron job
        if premature then
            return
        end
        local ok, err = ngx.timer.at(delay, handler)
        if ok then
            ngx.log(ngx.ERR, "Execute batch synchronization now!")
            ngx.log(ngx.ERR, "Work start")
            ngx.sleep(300)
            ngx.log(ngx.ERR, "Work complete")
        else
            ngx.log(ngx.ERR, "Failed to create the timer: ", err)
            return
        end
    end

    local ok, err = ngx.timer.at(delay, handler)
    if not ok then
        ngx.log(ngx.ERR, "failed to create the timer: ", err)
        return
    end
end

return M
