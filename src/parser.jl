# developed under  Julia 1.0.3
#
# functions for parsing .csv data from Schneider 

function parse_train_data(data_path::String; pv::Bool=true, load::Bool=true)
    
    data = Dict()
    fields = Dict()
    selection = String[]
    
    if pv
        push!(selection, "actual_pv")
    end
    if load
        push!(selection, "actual_consumption")
    end
    
    open(data_path, "r") do file
        for (i, line) in enumerate(eachline(file))
            
            #header: get fields position
            if i == 1
                header = split(line, ",")
                for (k, key) in enumerate(header)
                    if key in selection
                        fields[key] = k
                    end
                end
                continue
            end
            
            #other lines: collect information
            line = split(line, ",")
            timestamp = line[1]
            day, clock = split(timestamp, " ")
            day = Dates.Date(day)
            hour = parse(Int64, clock[1:2])
            minute = parse(Int64, clock[4:5])
            quater = Int(hour*4 + minute/15)
            
            if quater == 0
                day = day - Dates.Day(1)
                quater = 96
            end
               
            if !haskey(data, day)
                    data[day] = Dict("pv"=>Inf*ones(96), "load"=>Inf*ones(96), "total"=>0)
            end
            dict = data[day]
            if pv
                dict["pv"][quater] = parse(Float64, line[fields["actual_pv"]]) / 1000
            end
            if load
                dict["load"][quater] = parse(Float64, line[fields["actual_consumption"]]) / 1000
            end
            dict["total"] += 1
            data[day] = dict
            
        end
    end
    
    return data
    
end

function load_train_data(data_path::String)
    
    data = parse_train_data(data_path)
    pv = Array{Float64}(undef, 0, 0)
    load = Array{Float64}(undef, 0, 0)
    column = 1
    
    filters = Dict("weekday"=>Int64[], "weekend"=>Int64[], "winter"=>Int64[], "summer"=>Int64[])
    weekdays = 1:5
    summer = 5:9
    
    for day in keys(data)
        
        if data[day]["total"] != 96
            continue
        end
        
        if column == 1
            pv = reshape(data[day]["pv"], (:, 1))
            load = reshape(data[day]["load"], (:, 1))
        else
            pv = hcat(pv, reshape(data[day]["pv"], (:, 1)))
            load = hcat(load, reshape(data[day]["load"], (:, 1)))
        end
        
        day = Dates.Date(day)
        
        if Dates.dayofweek(day) in weekdays
            push!(filters["weekday"], column)
        else
            push!(filters["weekend"], column)
        end
        if Dates.month(day) in summer
            push!(filters["summer"], column)
        else
            push!(filters["winter"], column)
        end
        
        column += 1
    
    end
    
    return pv, load, filters
        
end

function load_test_periods(data_path::String)
    
    periods = Dict()
    fields = Dict()
    selection = ["price_buy_00", "price_sell_00", "period_id", "timestamp"]
    period = -1
    
    open(data_path, "r") do file
        for (i, line) in enumerate(eachline(file))
            
            #header: get fields position
            if i == 1
                header = split(line, ",")
                for (k, key) in enumerate(header)
                    if key in selection
                        fields[key] = k
                    end
                end
                continue
            end
            
            #other lines: collect information
            line = split(line, ",")
            current_period = line[fields["period_id"]]
            if period != current_period
                timestamp = line[fields["timestamp"]]
                day, time = split(timestamp, " ")
                date = Dates.DateTime(day*"T"*time)
                period = current_period
                periods[period] = Dict("t0"=>date,
                    "buy"=>[parse(Float64, line[fields["price_buy_00"]])],
                    "sell"=>[parse(Float64, line[fields["price_sell_00"]])], 
                    "total"=>1)
            else
                dict = periods[period]
                push!(dict["buy"], parse(Float64, line[fields["price_buy_00"]]))
                push!(dict["sell"], parse(Float64, line[fields["price_sell_00"]]))
                dict["total"] += 1
                periods[period] = dict
            end

        end
    end
    
    return periods
    
end
















function parse_raw_schneider(data_path::String)

	"""parse csv file to reorder data as time series
	data_path > path to csv
	return: Dict{String, Dict{String, Array{Float64}}}

	"""

 	selection = ["timestamp", "actual_consumption", "actual_pv", "load_00", "pv_00",
 	"price_buy_00", "price_sell_00", "period_id"]
	fields = Dict()
    data = Dict()
    k=0
    
    open(data_path, "r") do in_file
            
        for line in eachline(in_file)

            if k == 0
                header = split(line, ",")
                for (i, key) in enumerate(header)
                    if key in selection
                        fields[key] = i
                    end
                end
                k += 1
                continue
            end
            
            line = split(line, ",")
            
            period = line[fields["period_id"]]
            date = split(line[fields["timestamp"]], " ")
            day = date[1]
            timer = date[2]
            h = parse(Int64, timer[1:2])
            m = parse(Int64, timer[4:5])
            timer = Int(h*4 + m/15 + 1)

            if haskey(data, day) == false
                data[day] = Dict("load"=>zeros(96), "pv"=>zeros(96), 
                    "period"=>period, "load_forecast"=>zeros(96), "pv_forecast"=>zeros(96),
                	"price_buy"=>zeros(96), "price_sell"=>zeros(96), "total"=>0)
            end

            dict = data[day]
            dict["load"][timer] = parse(Float64, line[fields["actual_consumption"]]) / 1000
            dict["pv"][timer] = parse(Float64, line[fields["actual_pv"]]) / 1000
            dict["load_forecast"][timer] = parse(Float64, line[fields["load_00"]]) / 1000
            dict["pv_forecast"][timer] = parse(Float64, line[fields["pv_00"]]) / 1000
            dict["price_buy"][timer] = parse(Float64, line[fields["price_buy_00"]])
            dict["price_sell"][timer] = parse(Float64, line[fields["price_sell_00"]])
            dict["total"] += 1
            data[day] = dict

        end
        
    end

    uncomplete = String[]
    all_days = keys(data)

    for day in all_days

    	# remove uncomplete time series
    	if data[day]["total"] != 96
            push!(uncomplete, day)
            continue
        end

        # shift pv, load which are 15 min late
        load = data[day]["load"]
        pv = data[day]["pv"]
        data[day]["load"] = vcat(load[2:96, :], reshape(load[96, :], (1, :)))
        data[day]["pv"] = vcat(pv[2:96, :], reshape(pv[96, :], (1, :)))

    	next_day = string(Dates.Date(day) + Dates.Day(1))

    	if next_day in all_days
    		last_interval = data[next_day]["load"][1]
    		if last_interval != 0.
    			data[day]["load"][96] = last_interval
    		end
    	end

    end

    for day in uncomplete
    	delete!(data, day)
    end

	return data

end

function load_schneider(data_path::String; winter::Bool=true, summer::Bool=true,
	weekend::Bool=true, weekday::Bool=true, saturday::Bool=false, sunday::Bool=false,
    months::Array{Int64}=Int64[], days=String[])

	"""load data schneider: reorder and filter time series from raw csv file
	data_path > path to csv
	return: Dict{String, Array{Float64}}

	"""
    
    data = parse_raw_schneider(data_path)

	if winter
		append!(months, [1, 2, 3, 4, 10, 11, 12])
	end

	if summer
		append!(months, [5, 6, 7, 8, 9])
	end

	if weekday
		append!(days, ["Tuesday", "Wednesday", "Friday", "Thursday", "Monday"])
	end   

 	if weekend
 		append!(days, ["Saturday", "Sunday"])
 	end

 	if saturday
 		append!(days, ["Saturday"])
 	end

 	if sunday
 		append!(days, ["Sunday"])
 	end

    results = Dict{String, Array{Float64}}()

    for day in keys(data)

         month = parse(Int64, split(day, "-")[2])
			if !(month in months)
				continue
			end

		dayname = Dates.dayname(Dates.Date(day))
		if !(dayname in days)
			continue
		end

        scenario = data[day]

        if haskey(results, "pv") == false

            results["load"] = scenario["load"]
            results["pv"] = scenario["pv"]
            results["load_forecast"] = scenario["load_forecast"]
            results["pv_forecast"] = scenario["pv_forecast"]
            results["price_buy"] = scenario["price_buy"]
            results["price_sell"] = scenario["price_sell"]

        else

            results["load"] = hcat(results["load"], scenario["load"])
            results["pv"] = hcat(results["pv"], scenario["pv"])
            results["load_forecast"] = hcat(results["load_forecast"], scenario["load_forecast"])
            results["pv_forecast"] = hcat(results["pv_forecast"], scenario["pv_forecast"])
            results["price_buy"] = hcat(results["price_buy"], scenario["price_buy"])
            results["price_sell"] = hcat(results["price_sell"], scenario["price_sell"])

        end

    end

    return results

end

function load_prices(data_path::String)

    """load prices for one site csv file, sort periods with common prices
    data_path > path to csv
    return: Dict{Array{SubString{String},1},Dict{String,Array{Float64,1}}}

    """

    data = parse_raw_schneider(data_path)
    prices = Dict()

    for day in keys(data)

        if data[day]["total"] != 96
            continue
        end

        period = data[day]["period"]

        if !haskey(prices, period)
            prices[period] = Dict("buy"=>reshape(data[day]["price_buy"], (1, :)),
                    "sell"=>reshape(data[day]["price_sell"], (1, :)))
        else
            prices[period]["buy"] = vcat(prices[period]["buy"], data[day]["price_buy"]')
            prices[period]["sell"] = vcat(prices[period]["sell"], data[day]["price_sell"]')
        end

    end

    buy_scenario = Dict()
    sell_scenario = Dict()

    for period in keys(prices)

        buy = unique(prices[period]["buy"], dims=1)
        sell = unique(prices[period]["sell"], dims=1)

        if size(buy)[1] != 1
            println("WARNING: period $(period) has several buy prices")
            buy = findmax(prices[period]["buy"], dims=1)[1]
        end

        if size(sell)[1] != 1
            println("WARNING: period $(period) has several sell prices")
            sell = findmax(prices[period]["sell"], dims=1)[1]
        end

       if !haskey(buy_scenario, buy)
            buy_scenario[buy] = [period]
        else
            push!(buy_scenario[buy], period)
        end

        if !haskey(sell_scenario, sell)
            sell_scenario[sell] = [period]
        else
            push!(sell_scenario[sell], period)
        end

    end

    prices = Dict()

    for (price_buy, period_buy) in buy_scenario
        for (price_sell, period_sell) in sell_scenario

            period = intersect(period_buy, period_sell)
            if !isempty(period)
                prices[period] = Dict("buy"=>vec(price_buy), "sell"=>vec(price_sell))
            end

        end
    end
    
    return prices

end


function eval_forecasts(data_path::String)
    """
    """

    selection = ["timestamp", "actual_consumption", "actual_pv", "actual_consumption"]
    for t in 0:95
        #t = i*4
        if t < 10
            push!(selection, "pv_0"*string(t))
            push!(selection, "load_0"*string(t))
        else
            push!(selection, "pv_"*string(t))
            push!(selection, "load_"*string(t))
        end
        end

        fields = Dict()
        data = Dict()

        open(data_path, "r") do in_file
        k = 0
        for line in eachline(in_file)
            
            if k == 0
                header = split(line, ",")
                
                for (i, key) in enumerate(header)
                    if key in selection
                        fields[key] = i
                    end
                end
                k += 1
                continue
            end

            line = split(line, ",")
            date = split(line[1], " ")
            day, time = date
            date = Dates.DateTime(day*"T"*time)
            data[date] = line
        end
    end

    ###

    results = Dict(i=>Dict("pv"=>Float64[], "load"=>Float64[]) for i in 1:96);

    for (date, values) in data
        
        for i in 1:96
            
            hour = div(i, 4)
            minute = rem(i, 4)*15

            quater = date + Dates.Hour(hour) + Dates.Minute(minute)
            if !(quater in keys(data))
                continue
            end
            
            key_pv = "pv_"
            key_load = "load_"
            t = i-1
            if t < 10
                key_pv = key_pv*"0"*string(t)
                key_load = key_load*"0"*string(t)
            else
                key_pv = key_pv*string(t)
                key_load = key_load*string(t)
            end
            
            
            pv_forecast = parse(Float64, values[fields[key_pv]]) / 1000
            pv_real = parse(Float64, data[quater][fields["actual_pv"]]) / 1000
            push!(results[i]["pv"], pv_real-pv_forecast)
            
            load_forecast = parse(Float64, values[fields[key_load]]) / 1000
            load_real = parse(Float64, data[quater][fields["actual_consumption"]]) / 1000
            push!(results[i]["load"], load_real-load_forecast)
            
        end
        
    end

    return results

end