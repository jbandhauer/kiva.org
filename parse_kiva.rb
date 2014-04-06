require 'json'
require 'sequel'

DB = Sequel.connect('jdbc:mysql://localhost/kiva?user=root')

DB.drop_table?(:lender)
DB.drop_table?(:loan)
DB.drop_table?(:loan_description)

DB.create_table?(:lender) do
  column :lender_id,          String, :primary_key => true, :index => true
  column :name,               String
  column :image_id,           Integer
  column :image_template_id,  Integer
  column :whereabouts,        String
  column :country_code,       String
  column :uid,                String
  column :member_since,       DateTime
  column :personal_url,       String
  column :occupation,         String
  column :loan_because,       String, :text=>true
  column :occupational_info,  String, :text=>true
  column :loan_count,         Integer
  column :invitee_count,      Integer
  column :inviter_id,         String
end

DB.create_table?(:loan) do
  column :id,                            Integer, :primary_key => true, :index => true
  column :name,                          String
  column :status,                        String
  column :funded_amount,                 BigDecimal
  column :basket_amount,                 BigDecimal
  column :paid_amount,                   BigDecimal
  column :video,                         String
  column :activity,                      String
  column :sector,                        String
  column :theme,                         String
  column :use,                           String
  column :delinquent,                    TrueClass, :default=>false
  column :partner_id,                    Integer
  column :posted_date,                   DateTime
  column :planned_expiration_date,       DateTime
  column :loan_amount,                   BigDecimal
  column :lender_count,                  String
  column :currency_exchange_loss_amount, BigDecimal
  column :bonus_credit_eligibility,      TrueClass, :default=>false
  column :funded_date,                   DateTime
  column :paid_date,                     DateTime
  column :image_id,                      Integer
  column :image_template_id,             Integer

end

DB.create_table?(:loan_description) do
  primary_key :id

  column :loan_id,  Integer
  column :language, String
  column :text,     String, :text=>true
end

class Lender < Sequel::Model(:lender)
  unrestrict_primary_key
end

class Loan < Sequel::Model(:loan)
  unrestrict_primary_key
end

class LoanDescription < Sequel::Model(:loan_description)
end


#########################################################################################################


def process_lender(lender)
  model = Lender.new

  lender.each do |key, value|
    case key
    when :lender_id, :name, :whereabouts, :country_code, :uid, :member_since, :personal_url, :occupation,
         :loan_because, :occupational_info, :loan_count, :invitee_count, :inviter_id
      model[key] = value
    when :image
      value.each do |ikey, ivalue|
        case ikey
        when :id
          model[:image_id] = ivalue
        when :template_id
          model[:image_template_id] = ivalue
        else
          raise "unexpected key: #{ikey.to_s}"
        end
      end
    else
      raise "unexpected key: #{key.to_s}"
    end
  end
  raise "odd lender uuid #{lender[:uuid]} for #{lender[:lender_id]}" if lender[:uuid] && lender[:uuid] != lender[:lender_id]

  model.save
  model[:lender_id]
end

def process_loan(loan)
  model = Loan.new

  loan.each do |key, value|
    case key
    when :id, :name, :status, :funded_amount, :basket_amount, :paid_amount, :video, :activity, :sector,
         :theme, :use, :delinquent, :partner_id, :posted_date, :planned_expiration_date, :loan_amount,
         :lender_count, :currency_exchange_loss_amount, :bonus_credit_eligibility, :funded_date, :paid_date

      model[key] = value

    when :image
      value.each do |ikey, ivalue|
        case ikey
        when :id
          model[:image_id] = ivalue
        when :template_id
          model[:image_template_id] = ivalue
        else
          raise "unexpected key: #{ikey} in #{key}"
        end
      end
    when :description
      # create_loan_description(value, model[:id])
    else
      # raise "unexpected key: #{key}"
    end
  end

  model.save
  model[:id]
end

def create_loan_description(description, loan_id)
  description.each do |key, value|
    case key
    when :texts
      value.each do |ikey, ivalue|
        LoanDescription.new(:loan_id => loan_id, :language => ikey.to_s, :text => ivalue).save
      end
    when :languages
      # ignore
    else
      raise "unexpected key: #{key} in description"
    end
  end
end

=begin

    {
      "location": {
        "country_code": "PE",
        "country": "Peru",
        "town": null,
        "geo": {
          "level": "country",
          "pairs": "-10 -76",
          "type": "point"
        }
      },

      "borrowers": [
        {
          "first_name": "Anonymous",
          "last_name": "",
          "gender": "M",
          "pictured": true
        }
      ],

      "terms": {
        "disbursal_date": "2010-10-22T07:00:00Z",
        "disbursal_currency": "PEN",
        "disbursal_amount": 2000,
        "repayment_interval": "Monthly",
        "repayment_term": 14,
        "loan_amount": 725,
        "local_payments": [
          {
            "due_date": "2010-11-22T08:00:00Z",
            "amount": 137.46
          },
        ],
        "scheduled_payments": [
          {
            "due_date": "2011-12-01T08:00:00Z",
            "amount": 71.12
          }
        ],
        "loss_liability": {
          "nonpayment": "lender",
          "currency_exchange": "shared",
          "currency_exchange_coverage_rate": 0.2
        }
      },
      "payments": [
        {
          "amount": 16.38,
          "local_amount": 45.19,
          "processed_date": "2012-01-31T08:00:00Z",
          "settlement_date": "2012-02-27T08:37:21Z",
          "rounded_local_amount": 45.95,
          "currency_exchange_loss_amount": 0,
          "payment_id": 257395716,
          "comment": null
        }
      ],
      "journal_totals": {
        "entries": 0,
        "bulkEntries": 00
      },
      "translator": {
        "byline": "Jennifer Day",
        "image": null
      }
    },
=end


def process_file(type, name)
  file = File.open(name, "r")
  content = JSON.parse(file.read, symbolize_names: true)
  file.close

  items = content[(type.to_s+"s").to_sym]
  items.each do |item|
    send("process_"+type.to_s, item)
    print '.'
  end
  puts
  items.count
end

def process_files(type, pattern)
  count = 0
  Dir[pattern].each do |fname|
    puts "\n#{fname}"
    count += process_file(type, fname)
  end
  count
end

# count = process_file(:lender, "data/lenders/500.json")
# count = process_files(:lender, "data/lenders/50*.json")

count = process_file(:loan, "data/loans/500.json")

puts "total_items: #{count}"