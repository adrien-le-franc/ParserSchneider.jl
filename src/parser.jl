# developed under  Julia 1.0.3
#
# functions for parsing .json data from Schneider 


function clean_data_schneider(json_file::String, new_json::String)

	"""clean data schneider: clean and reorder daylong time series then save data dict as .json
	json_file > original json data
	new_json > name for clean json file

	"""

	raw = JSON.parsefile(json_file)

	data = Dict(2=>Dict(), 32=>Dict())
	n_data = length(raw)
	total = 0

	@showprogress for i in 1:n_data
	    
	    fields = raw[i]["fields"]
	    
	    if length(fields) != 199
	        continue
	    end
	    
	    total += 1
	    
	    site = fields["siteid"]
	    date = split(fields["timestamp"], "T")
	    day = date[1]
	    time = date[2]
	    h = parse(Int64, time[1:2])
	    m = parse(Int64, time[4:5])
	    time = Int(h*4 + m/15 + 1)
	    
	    if haskey(data[site], day) == false 
	        
	        data[site][day] = Dict("pv"=>zeros(96), "load"=>zeros(96), "sale_price"=>zeros(96), 
	            "purchase_price"=>zeros(96), "pv_forecast_15"=>zeros(96),
	            "load_forecast_15"=>zeros(96), "total"=>0)
	        
	    end
	       
	    # energy data stored in kWh  

	    dict = data[site][day]
	    dict["pv"][time] = fields["pv_values"] / 1000
	    dict["load"][time] = fields["load_values"] / 1000 
	    dict["sale_price"][time] = fields["sale_price"] 
	    dict["purchase_price"][time] = fields["purchase_price"]
	    dict["pv_forecast_15"][time] = fields["pv_forecast_15"] / 1000
	    dict["load_forecast_15"][time] = fields["load_forecast_15"] / 1000
	    dict["total"] += 1
	    data[site][day] = dict
	    
	end

	# remove uncomplete time series 
	for id in keys(data)

	    for key in keys(data[id])

	        n_intervals = data[id][key]["total"]
	        if n_intervals != 96
	            delete!(data[id], key)
	        end

	    end
	end

	file = JSON.json(data)
	open(new_json,"w") do f 
	    write(f, file) 
	end

end

function load_schneider(clean_json::String; site_id::Union{Int64, Tuple{Vararg{Int64}}}=(2, 32),
	winter::Bool=true, summer::Bool=true, weekend::Bool=true, weekday::Bool=true)

	"""load schneider data: return daylong time series of field
	clean_json > clean json file
	field > "pv", "load", "sale_price", "purchase_price"
	site_id > 2, 32, (2, 32)
	season > winter, summer, both in default mode

	"""

	data = JSON.parsefile(clean_json)

	dict = Dict("pv"=>0, "load"=>0, "pv_forecast_15"=>0, "load_forecast_15"=>0,
		"sale_price"=>0, "purchase_price"=>0, "dates"=>String[], "sites"=>String[],
		"seasons"=>String[], "days"=>String[])

	months = []
	days = []

	if winter
		push!(dict["seasons"], "winter")
		append!(months, [1, 2, 3, 4, 5, 10, 11, 12])
	end

	if summer
		push!(dict["seasons"], "summer")
		append!(months, [6, 7, 8, 9])
	end

	if weekday
		push!(dict["days"], "weekday")
		append!(days, ["Tuesday", "Wednesday", "Friday", "Thursday", "Monday"])
	end   

 	if weekend
 		push!(dict["days"], "weekend")
 		append!(days, ["Saturday", "Sunday"])
 	end

	for id in site_id

		push!(dict["sites"], string(id))
		site = data[string(id)]

		for date in keys(site)

			month = parse(Int64, split(date, "-")[2])
			if !(month in months)
				continue
			end

			day = Dates.dayname(Dates.Date(date))
			if !(day in days)
				continue
			end

			push!(dict["dates"], date)
			scenario = site[date]

			if dict["pv"] == 0

				dict["pv"] = scenario["pv"]
				dict["load"] = scenario["load"]
				dict["pv_forecast_15"] = scenario["pv_forecast_15"]
				dict["load_forecast_15"] = scenario["load_forecast_15"]
				dict["sale_price"] = scenario["sale_price"]
				dict["purchase_price"] = scenario["purchase_price"]

			else

				dict["pv"] = hcat(dict["pv"], scenario["pv"])
				dict["load"] = hcat(dict["load"], scenario["load"])
				dict["pv_forecast_15"] = hcat(dict["pv_forecast_15"], scenario["pv_forecast_15"])
				dict["load_forecast_15"] = hcat(dict["load_forecast_15"], scenario["load_forecast_15"])
				dict["sale_price"] = hcat(dict["sale_price"], scenario["sale_price"])
				dict["purchase_price"] = hcat(dict["purchase_price"], scenario["purchase_price"])

			end

		end

	end

	if dict["pv"] == 0
			error("selected arguments result in empty data set")
	end

	for k in ["pv", "load", "pv_forecast_15", "load_forecast_15", "sale_price", "purchase_price"]
		dict[k] = convert(Array{Float64}, dict[k])
	end

	return dict

end