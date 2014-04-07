require 'json'
require 'sequel'

EMIT_PLANNED_PAYMENTS = false

DB = Sequel.connect('jdbc:mysql://localhost/kiva?user=root')

def do_drop_tables
  [:lender, :loan, :description, :payment, :local_payment, :scheduled_payment, :terms,
   :borrower, :location, :loan_lender
  ].each {|table| DB.drop_table?(table)}
end

def do_create_tables
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
    column :funded_amount,                 String #BigDecimal, :size=>[20, 2]
    column :basket_amount,                 String #BigDecimal, :size=>[20, 2]
    column :paid_amount,                   String #BigDecimal, :size=>[20, 2]
    column :video,                         String
    column :activity,                      String
    column :sector,                        String
    column :theme,                         String
    column :use,                           String, :text=>true
    column :delinquent,                    TrueClass, :default=>false
    column :partner_id,                    Integer
    column :posted_date,                   DateTime
    column :planned_expiration_date,       DateTime
    column :loan_amount,                   String #BigDecimal, :size=>[20, 2]
    column :lender_count,                  String
    column :currency_exchange_loss_amount, String #BigDecimal, :size=>[20, 2]
    column :bonus_credit_eligibility,      TrueClass, :default=>false
    column :funded_date,                   DateTime
    column :paid_date,                     DateTime
    column :image_id,                      Integer
    column :image_template_id,             Integer
    column :translator_byline,             String
    column :translator_image,              Integer
    column :journal_totals_entries,        Integer
    column :journal_totals_bulkEntries,    Integer

  end

  DB.create_table?(:description) do
    primary_key :id
    column :loan_id,  Integer, :index => true

    column :language, String
    column :text,     String, :text=>true
  end

  DB.create_table?(:payment) do
    column :payment_id,                    Integer, :primary_key => true, :index => true
    column :loan_id,                       Integer, :index => true

    column :amount,                        String #BigDecimal, :size=>[20, 2]
    column :local_amount,                  String #BigDecimal, :size=>[20, 2]
    column :processed_date,                DateTime
    column :settlement_date,               DateTime
    column :rounded_local_amount,          String #BigDecimal, :size=>[20, 2]
    column :currency_exchange_loss_amount, String #BigDecimal
    column :comment,                       String, :text=>true
  end

  DB.create_table?(:local_payment) do
    primary_key :id
    column :terms_id,                      Integer, :index => true

    column :due_date,                      DateTime
    column :amount,                        String #BigDecimal, :size=>[20, 2]
  end

  DB.create_table?(:scheduled_payment) do
    primary_key :id
    column :terms_id,                      Integer, :index => true

    column :due_date,                      DateTime
    column :amount,                        String #BigDecimal, :size=>[20, 2]
  end

  DB.create_table?(:terms) do
    primary_key :id
    column :loan_id,                       Integer, :index => true

    column :disbursal_date,                                 DateTime
    column :disbursal_currency,                             String
    column :disbursal_amount,                               String #BigDecimal, :size=>[20, 2]
    column :repayment_interval,                             String
    column :repayment_term,                                 Integer
    column :loan_amount,                                    String #BigDecimal, :size=>[20, 2]
    column :loss_liability_nonpayment,                      String
    column :loss_liability_currency_exchange,               String
    column :loss_liability_currency_exchange_coverage_rate, String
  end

  DB.create_table?(:borrower) do
    primary_key :id
    column :loan_id,        Integer, :index => true

    column :first_name,     String
    column :last_name,      String
    column :gender,         String
    column :pictured,       TrueClass, :default=>false
  end

  DB.create_table?(:location) do
    primary_key :id
    column :loan_id,        Integer, :index => true

    column :country_code,   String
    column :country,        String
    column :town,           String
    column :geo_level,      String
    column :geo_pairs,      String
    column :geo_type,       String
  end

  DB.create_table?(:loan_lender) do
    primary_key :id

    column :loan_id,        Integer, :index => true
    column :lender_id,      String,  :index => true
  end
end


#########################################################################################################

class Lender < Sequel::Model(:lender)
  unrestrict_primary_key
end

class Loan < Sequel::Model(:loan)
  unrestrict_primary_key
end

class LoanDescription < Sequel::Model(:description)
end

class Payment < Sequel::Model(:payment)
  unrestrict_primary_key
end

class LocalPayment < Sequel::Model(:local_payment)
end

class ScheduledPayment < Sequel::Model(:scheduled_payment)
end

class Terms < Sequel::Model(:terms)
end

class Borrower < Sequel::Model(:borrower)
end

class Location < Sequel::Model(:location)
end

class LoanLender < Sequel::Model(:loan_lender)
end

#########################################################################################################
def v(val)
  val.to_s if not val.nil?
end

def process_lenders(lender)
  model = Lender.new

  lender.each do |key, value|
    case key
    when :lender_id, :name, :whereabouts, :country_code, :uid, :member_since, :personal_url, :occupation,
         :loan_because, :occupational_info, :loan_count, :invitee_count, :inviter_id
      model[key] = v(value)
    when :image
      value.each do |key2, value2|
        case key2
        when :id, :template_id
          model[(key.to_s+'_'+key2.to_s).to_sym] = v(value2)
        else
          raise "unexpected key: #{key2.to_s}"
        end
      end
    else
      raise "unexpected key: #{key.to_s}"
    end
  end
  raise "odd lender uuid #{lender[:uuid]} for #{lender[:lender_id]}" if lender[:uuid] && lender[:uuid] != lender[:lender_id]

  model.save(:validate => false, :changed => true)
  model[:lender_id]
end

def process_loans(loan)
  model = Loan.new

  loan.each do |key, value|
    case key
    when :id, :name, :status, :funded_amount, :basket_amount, :paid_amount, :video, :activity, :sector,
         :theme, :use, :delinquent, :partner_id, :posted_date, :planned_expiration_date, :loan_amount,
         :lender_count, :currency_exchange_loss_amount, :bonus_credit_eligibility, :funded_date, :paid_date
      model[key] = v(value)
    when :image
      value.each do |key2, value2|
        case key2
        when :id, :template_id
          model[(key.to_s+'_'+key2.to_s).to_sym] = v(value2)
        else
          raise "unexpected key: #{key2} in #{key}"
        end
      end
    when :translator
      value.each do |key2, value2|
        case key2
        when :byline, :image
          model[(key.to_s+'_'+key2.to_s).to_sym] = v(value2)
        else
          raise "unexpected key: #{key2} in #{key}"
        end
      end if value

    when :journal_totals
      value.each do |key2, value2|
        case key2
        when :entries, :bulkEntries
          model[(key.to_s+'_'+key2.to_s).to_sym] = v(value2)
        else
          raise "unexpected key: #{key2} in #{key}"
        end
      end

    when :description
      create_description(value, model[:id])
    when :payments
      create_payments(value, model[:id])
    when :terms
      create_terms(value, model[:id])
    when :borrowers
      create_borrowers(value, model[:id])
    when :location
      create_location(value, model[:id])
    else
      raise "unexpected key: #{key} in loan #{loan}"
    end
  end

  model.save(:validate => false, :changed => true)
  model[:id]
end

def process_loans_lenders(loan_lender)
  loan_id = lender_ids = 0
  loan_lender.each do |key, value|
    case key
    when :id
      loan_id = value.to_i
    when :lender_ids
      lender_ids = value
    else
      raise "unexpected key: #{key} in loan_lender #{loan_lender}"
    end
  end

  lender_ids.each do |lender_id|
    LoanLender.new(:loan_id => loan_id, :lender_id => lender_id).save(:validate => false, :changed => true)
  end if loan_id and lender_ids
end

#########################################################################################################

def create_description(descriptions, loan_id)
  descriptions.each do |key, value|
    case key
    when :texts
      value.each do |key2, value2|
        LoanDescription.new(:loan_id => loan_id, :language => key2.to_s, :text => v(value2)).save(:validate => false, :changed => true)
      end
    when :languages
      # ignore
    else
      raise "unexpected key: #{key} in description"
    end
  end
end

def create_payments(payments, loan_id)
  payments.each do |payment|
    model = Payment.new(:loan_id => loan_id)
    payment.each do |key, value|
      case key
      when :payment_id, :amount, :local_amount, :processed_date, :settlement_date, :rounded_local_amount,
           :currency_exchange_loss_amount, :comment
        model[key] = v(value)
      else
        raise "unexpected key: #{key} in payment #{payment}"
      end
    end
    model.save(:validate => false, :changed => true)
  end
end

def create_local_payments(payments, terms_id)
  payments.each do |payment|
    model = LocalPayment.new(:terms_id => terms_id)
    payment.each do |key, value|
      case key
      when :due_date, :amount
        model[key] = v(value)
      else
        raise "unexpected key: #{key} in local_payment #{payment}"
      end
    end
    model.save(:validate => false, :changed => true)
  end
end

def create_scheduled_payments(payments, terms_id)
  payments.each do |payment|
    model = ScheduledPayment.new(:terms_id => terms_id)
    payment.each do |key, value|
      case key
      when :due_date, :amount
        model[key] = v(value)
      else
        raise "unexpected key: #{key} in scheduled_payment #{payment}"
      end
    end
    model.save(:validate => false, :changed => true)
  end
end

def create_terms(terms, loan_id)
  # eager save to generate autoincrement :id used below
  model = Terms.new(:loan_id => loan_id).save(:validate => false, :changed => true)
  terms.each do |key, value|
    case key
    when :disbursal_date, :disbursal_currency, :disbursal_amount, :repayment_interval,
         :repayment_term, :loan_amount
      model[key] = v(value)
    when :local_payments
      create_local_payments(value, model[:id]) if EMIT_PLANNED_PAYMENTS
    when :scheduled_payments
      create_scheduled_payments(value, model[:id]) if EMIT_PLANNED_PAYMENTS
    when :loss_liability
      value.each do |key2, value2|
        case key2
        when :nonpayment, :currency_exchange, :currency_exchange_coverage_rate
          model[(key.to_s+'_'+key2.to_s).to_sym] = v(value2)
        else
          raise "unexpected key: #{key2} in #{key} in terms"
        end
      end
    else
      raise "unexpected key: #{key} in description"
    end
  end
  model.save(:validate => false, :changed => true)
end

def create_borrowers(borrowers, loan_id)
  borrowers.each do |borrower|
    model = Borrower.new(:loan_id => loan_id)
    borrower.each do |key, value|
      case key
      when :first_name, :last_name, :gender, :pictured
        model[key] = v(value)
      else
        raise "unexpected key: #{key} in borrower"
      end
    end
    model.save(:validate => false, :changed => true)
  end
end

def create_location(location, loan_id)
  model = Location.new(:loan_id => loan_id)
  location.each do |key, value|
    case key
    when :country_code, :country, :town
      model[key] = v(value)
    when :geo
      value.each do |key2, value2|
        case key2
        when :level, :pairs, :type
          model[(key.to_s+'_'+key2.to_s).to_sym] = v(value2)
        else
          raise "unexpected key: #{key2} in #{key}"
        end
      end
    else
      raise "unexpected key: #{key} in location"
    end
  end
  model.save(:validate => false, :changed => true)
end

def process_file(type, name)
  file = File.open(name, "r")
  content = JSON.parse(file.read, symbolize_names: true)
  file.close

  items = content[type]
  DB.transaction do
    items.each do |item|
      begin
        send("process_"+type.to_s, item)
      rescue
        puts "Error in #{name}"
        #puts content.to_s
        raise
      end
      print '.'
    end
  end

  puts
  items.count
end

def process_files(type, pattern)
  count = 0
  Dir[pattern].each do |fname|
    puts "\n#{fname}"
    count += process_file(type, fname)
    puts "items: #{count}"
  end
  count
end

#########################################################################################################

# count = process_file(:lenders, "data/lenders/500.json")
# count = process_files(:lenders, "data/lenders/50*.json")

# count = process_file(:loans, "data/loans/500.json")
# count = process_files(:loans, "data/loans/50*.json")
# count = process_files(:loans, "data/loans/1227.json")

# count = process_file(:loans_lenders, "data/loans_lenders/50.json")
# count = process_files(:loans_lenders, "data/loans_lenders/5*.json")

do_drop_tables
do_create_tables

count = 0
count += process_files(:loans, "data/loans/*.json")
count += process_files(:lenders, "data/lenders/*.json")
count += process_files(:loans_lenders, "data/loans_lenders/*.json")

puts "total_items: #{count}"

