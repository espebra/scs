local kidclass = {};
 
kidclass.__call = function (class, ...)
    local instance = {};
    if class.Constructor then
        class.Constructor(instance, ...);
    end
 
    class.__index = class;
    setmetatable(instance, class);
    return instance;
end
 
function kidclass.new()
    local class = {};
    setmetatable(class, kidclass);
    return class;
end
 
return kidclass;
