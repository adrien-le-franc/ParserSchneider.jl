# developed under julia 1.1.1
#
# functions for managing .csv data from Schneider


function normalize(data::Array{Float64}, highest::Float64, lowest::Float64)
    return (data .- lowest) / (highest - lowest)
end

function string_to_date(timestamp::String)
    day, timing = split(timestamp, " ")
    return Dates.DateTime(day*"T"*timing)
end

function energy_price()
   
    df = DataFrame(timestamp=Dates.Time[], buy=Float64[], sell=Float64[])

    for timestamp in Dates.Time(0, 0, 0):Dates.Minute(15):Dates.Time(23, 45, 0)

        if timestamp <= Dates.Time(7, 0, 0)
            buy = 0.13
        elseif timestamp <= Dates.Time(22, 0, 0)
            buy = 0.17
        else
            buy = 0.13
        end
        
        sell = 0.07
        
        push!(df, [timestamp, buy, sell])

    end
    
    return df
    
end