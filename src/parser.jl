# developed under julia 1.1.1
#
# functions for managing .csv data from Schneider

include("utils.jl")


"""
functions to process raw .csv data from Schneider into 
a unified control DataFrame format 
"""

function reprocess_control_dataframe(control::DataFrame, site_id::Int64, price::DataFrame)
   
    new_control_df = initialize_control_like_dataframe(control)
    period_id = 0
    
    for (row_id, df) in enumerate(eachrow(control))
        
        timestamp = string_to_date(df[:timestamp])
        timing = Dates.Time(timestamp)
        
        if timing != Dates.Time(0, 0, 0) && period_id == 0
            continue
        elseif timing == Dates.Time(0, 0, 0)
            period_id += 1
        end
            
        row = Any[]
        push!(row, timestamp)
        push!(row, site_id)
        push!(row, period_id)    
        append!(row, Array{Float64}(collect(df)[4:end]))
        
        for k in 1:96
            
            row[197+k] = price[price.timestamp .== timing + Dates.Minute(15*(k-1)), :buy][1]
            row[293+k] = price[price.timestamp .== timing + Dates.Minute(15*(k-1)), :sell][1]
            
        end
        
        push!(new_control_df, row)
        
    end
    
    return new_control_df
    
end

function convert_forcast_to_control_dataframe(forecast::DataFrame, site_id::Int64, 
    price::DataFrame, control_example::DataFrame)
   
    control_data = initialize_control_like_dataframe(control_example)
    period_id = 0
    last_row = size(forecast)[1] - 96
    
    for (row_id, df) in enumerate(eachrow(forecast))
        
        timestamp = string_to_date(df[:Timestamp])
        timing = Dates.Time(timestamp)
        
        if timing != Dates.Time(0, 0, 0) && period_id == 0
            continue
        elseif timing == Dates.Time(0, 0, 0)
            period_id += 1
        end
            
        row = Any[]
        push!(row, timestamp)
        push!(row, site_id)
        push!(row, period_id)
        push!(row, df[:Load])
        push!(row, df[:PV])
        append!(row, zeros(96*4))
        
        for k in 1:96
            
            load_forecast_k = forecast[row_id+k, 5+k]
            pv_forecast_k = forecast[row_id+k, 101+k]
            
            row[5+k] = load_forecast_k
            row[101+k] = pv_forecast_k
            row[197+k] = price[price.timestamp .== timing + Dates.Minute(15*(k-1)), :buy][1]
            row[293+k] = price[price.timestamp .== timing + Dates.Minute(15*(k-1)), :sell][1]
            
        end
        
        push!(control_data, row)
        
        if row_id == last_row
            break
        end
        
    end
    
    return control_data
    
end

function remove_uncomplete_periods!(data::DataFrame)
    
    k_max = last(data)[:period_id]

    for k =1:k_max
        frames = size(data[data.period_id .== k, :])[1]
        if frames != 96
            deleterows!(data, data.period_id .== k)
        end
    end
    
    return data
    
end


"""
functions for computing MAE & RMSE on normalized data
"""

function normalize_control_data!(data::DataFrame)
    
    pv = data[:, :actual_pv]
    min_pv = minimum(pv)
    max_pv = maximum(pv)
    data[:actual_pv] = normalize(pv, max_pv, min_pv)

    load = data[:, :actual_consumption]
    min_load = minimum(load)
    max_load = maximum(load)
    data[:actual_consumption] = normalize(load, max_load, min_load)

    for k in 0:95
        
        quater = string(k)
        if k < 10
            quater = "0"*quater
        end
        
        data[Symbol("pv_$(quater)")] = normalize(data[:, Symbol("pv_$(quater)")], max_pv, min_pv)
        data[Symbol("load_$(quater)")] = normalize(data[:, Symbol("load_$(quater)")], max_load, min_load)
        
    end
    
    return data
    
end

function compute_error(data::DataFrame)

    data = normalize_control_data!(data)
    load_error = Float64[]
    pv_error = Float64[]
    
    for (row_id, df) in enumerate(eachrow(data))

        if row_id < 97
            continue
        end

        load = df[:actual_consumption]
        pv = df[:actual_pv]
        
        for k in 0:95

            quater = string(k)
            if k < 10
                quater = "0"*quater
            end

            push!(load_error, load - data[row_id-k-1, Symbol("load_$(quater)")])
            push!(pv_error, pv - data[row_id-k-1, Symbol("pv_$(quater)")])

        end

    end
    
    n_data = length(load_error)
    results = DataFrame() 
    
    results[:site_id] = data[1, :site_id]
    results[:load_rmse] = sqrt(sum(load_error.^2) / n_data)
    results[:pv_rmse] = sqrt(sum(pv_error.^2) / n_data)
    results[:load_mae] = sum(abs.(load_error)) / n_data
    results[:pv_mae] = sum(abs.(pv_error)) / n_data
     
    return results
    
end