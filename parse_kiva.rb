require 'json'
require 'sequel'

DO_DROP = true


DB = Sequel.connect('jdbc:mysql://localhost/kiva?user=root')

if DO_DROP
  DB.drop_table?(:lender)
  DB.drop_table?(:loan)
  DB.drop_table?(:description)
  DB.drop_table?(:payment)
  DB.drop_table?(:local_payment)
  DB.drop_table?(:scheduled_payment)
  DB.drop_table?(:terms)
  DB.drop_table?(:borrower)
end

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
  column :funded_amount,                 BigDecimal, :size=>[10, 2]
  column :basket_amount,                 BigDecimal, :size=>[10, 2]
  column :paid_amount,                   BigDecimal, :size=>[10, 2]
  column :video,                         String
  column :activity,                      String
  column :sector,                        String
  column :theme,                         String
  column :use,                           String
  column :delinquent,                    TrueClass, :default=>false
  column :partner_id,                    Integer
  column :posted_date,                   DateTime
  column :planned_expiration_date,       DateTime
  column :loan_amount,                   BigDecimal, :size=>[10, 2]
  column :lender_count,                  String
  column :currency_exchange_loss_amount, BigDecimal, :size=>[10, 2]
  column :bonus_credit_eligibility,      TrueClass, :default=>false
  column :funded_date,                   DateTime
  column :paid_date,                     DateTime
  column :image_id,                      Integer
  column :image_template_id,             Integer

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

  column :amount,                        BigDecimal, :size=>[10, 2]
  column :local_amount,                  BigDecimal, :size=>[10, 2]
  column :processed_date,                DateTime
  column :settlement_date,               DateTime
  column :rounded_local_amount,          BigDecimal, :size=>[10, 2]
  column :currency_exchange_loss_amount, BigDecimal
  column :comment,                       String
end

DB.create_table?(:local_payment) do
  primary_key :id
  column :terms_id,                      Integer, :index => true

  column :due_date,                      DateTime
  column :amount,                        BigDecimal, :size=>[10, 2]
end

DB.create_table?(:scheduled_payment) do
  primary_key :id
  column :terms_id,                      Integer, :index => true

  column :due_date,                      DateTime
  column :amount,                        BigDecimal, :size=>[10, 2]
end

DB.create_table?(:terms) do
  primary_key :id
  column :loan_id,                       Integer, :index => true

  column :disbursal_date,                                 DateTime
  column :disbursal_currency,                             String
  column :disbursal_amount,                               BigDecimal, :size=>[10, 2]
  column :repayment_interval,                             String
  column :repayment_term,                                 Integer
  column :loan_amount,                                    BigDecimal, :size=>[10, 2]
  column :loss_liability_nonpayment,                      String
  column :loss_liability_currency_exchange,               String
  column :loss_liability_currency_exchange_coverage_rate, BigDecimal
end

DB.create_table?(:borrower) do
  primary_key :id
  column :loan_id,        Integer, :index => true

  column :first_name,     String
  column :last_name,      String
  column :gender,         String
  column :pictured,       TrueClass, :default=>false
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

#########################################################################################################


def process_lender(lender)
  model = Lender.new

  lender.each do |key, value|
    case key
    when :lender_id, :name, :whereabouts, :country_code, :uid, :member_since, :personal_url, :occupation,
         :loan_because, :occupational_info, :loan_count, :invitee_count, :inviter_id
      model[key] = value.to_s
    when :image
      value.each do |key2, value2|
        case key2
        when :id
          model[:image_id] = value2.to_s
        when :template_id
          model[:image_template_id] = value2.to_s
        else
          raise "unexpected key: #{key2.to_s}"
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

      model[key] = value.to_s

    when :image
      value.each do |key2, value2|
        case key2
        when :id, :template_id
          model[("image_"+key2.to_s).to_sym] = value2.to_s
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
    else
      # raise "unexpected key: #{key}"
    end
  end

  model.save
  model[:id]
end

def create_description(descriptions, loan_id)
  descriptions.each do |key, value|
    case key
    when :texts
      value.each do |key2, value2|
        LoanDescription.new(:loan_id => loan_id, :language => key2.to_s, :text => value2.to_s).save
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
        model[key] = value.to_s
      else
        raise "unexpected key: #{key} in payment #{payment}"
      end
    end
    model.save
  end
end

def create_local_payments(payments, terms_id)
  payments.each do |payment|
    model = LocalPayment.new(:terms_id => terms_id)
    payment.each do |key, value|
      case key
      when :due_date, :amount
        model[key] = value.to_s
      else
        raise "unexpected key: #{key} in local_payment #{payment}"
      end
    end
    model.save
  end
end

def create_scheduled_payments(payments, terms_id)
  payments.each do |payment|
    model = ScheduledPayment.new(:terms_id => terms_id)
    payment.each do |key, value|
      case key
      when :due_date, :amount
        model[key] = value.to_s
      else
        raise "unexpected key: #{key} in scheduled_payment #{payment}"
      end
    end
    model.save
  end
end

def create_terms(terms, loan_id)
  # eager save to generate autoincrement :id used below
  model = Terms.new(:loan_id => loan_id).save
  terms.each do |key, value|
    case key
    when :disbursal_date, :disbursal_currency, :disbursal_amount, :repayment_interval,
         :repayment_term, :loan_amount
      model[key] = value.to_s
    when :local_payments
      create_local_payments(value, model[:id])
    when :scheduled_payments
      create_scheduled_payments(value, model[:id])
    when :loss_liability
      value.each do |key2, value2|
        case key2
        when :nonpayment, :currency_exchange, :currency_exchange_coverage_rate
          model[("loss_liability_"+key2.to_s).to_sym] = value2.to_s
        else
          raise "unexpected key: #{key2} in #{key} in terms"
        end
      end
    else
      raise "unexpected key: #{key} in description"
    end
  end
  model.save
end

def create_borrowers(borrowers, loan_id)
  borrowers.each do |borrower|
    model = Borrower.new(:loan_id => loan_id)
    borrower.each do |key, value|
      case key
      when :first_name, :last_name, :gender, :pictured
        model[key] = value.to_s
      else
        raise "unexpected key: #{key} in borrower"
      end
    end
    model.save
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
    begin
      send("process_"+type.to_s, item)
    rescue
      puts "Error in #{name}"
      #puts content.to_s
      raise
    end
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