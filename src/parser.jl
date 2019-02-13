# developed under  Julia 1.0.3
#
# functions for parsing .json data from Schneider 


function parser_raw_schneider(data_path::String)

	"""parse csv file to reorder data as time series
	data_path > path to csv
	return: Dict{String, Dict{String, Array{Float64}}}

	"""

 	selection = ["timestamp", "actual_consumption", "actual_pv", "load_00", "pv_00",
 	"price_buy_00", "price_sell_00"]
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
            
            date = split(line[fields["timestamp"]], " ")
            day = date[1]
            timer = date[2]
            h = parse(Int64, timer[1:2])
            m = parse(Int64, timer[4:5])
            timer = Int(h*4 + m/15 + 1)

            if haskey(data, day) == false
                data[day] = Dict("load"=>zeros(96), "pv"=>zeros(96),
                	"load_forecast"=>zeros(96), "pv_forecast"=>zeros(96),
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
	weekend::Bool=true, weekday::Bool=true, saturday::Bool=true, sunday::Bool=true)

	"""load data schneider: reorder and filter time series from raw csv file
	data_path > path to csv
	return: Dict{String, Array{Float64}}

	"""
    
    data = parser_raw_schneider(data_path)

    months = Int64[]
	days = String[]

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


