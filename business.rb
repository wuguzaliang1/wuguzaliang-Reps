module Business
  class Base < ActiveRecord::Base
    establish_connection :business
    self.abstract_class = true
  end

  class Buser < Business::Base
    has_many :busers_roles, class_name: 'Business::BusersRole', foreign_key: 'buser_id'
    has_many :roles, through: :busers_roles
    has_many :business_manager_months, class_name: 'Business::BusinessManagerMonths', foreign_key: 'buser_id'
    has_many :reporting_manager_months, class_name: 'Business::ReportingManagerMonths', foreign_key: 'buser_id'
    # edit again
    
    # edit again
    # edit again
    # edit again
    def self.sync
      all.each do |buser|
        next unless buser.active? || (!buser.inactive_date.nil? && buser.inactive_date.to_date >= '2018-01-01'.to_date)
        role_ids = (buser.role_ids & [1, 8]).any? ? buser.role_ids + [14] : buser.role_ids
        r_and_r = {roles: ::Role.find(role_ids)}
        r_and_r[:notes_id] = buser.notes_id.strip
        r_and_r[:onboard_date] = buser.onboard_date&.in_time_zone('Asia/Shanghai') || '1990-01-01'.to_date
        r_and_r[:separation_date] = if buser.active || buser.inactive_date
                                      buser.inactive_date&.in_time_zone('Asia/Shanghai')
                                    else
                                      '2017-12-31'.to_date
                                    end

        if (user = User.find_by_id(buser.id)).present?
          puts " - Update: #{buser.name}"
          user.update(buser.as_json(
              only: [:name, :also_known_as, :email, :site_id, :type_id]
          ).merge(r_and_r))
        else
          puts " - Create: #{buser.name}"
          user = User.new(buser.as_json(
              only: [:id, :name, :also_known_as, :email, :site_id, :type_id]
          ).merge(r_and_r))
          user.save
        end

        buser.business_manager_months.each do |bm|
          year = bm['Year'].to_i
          managers = {}
        # a comments to test
          BusinessManagerMonths::MONTHS.each do |month, column|
            manager_id = bm[column].to_i

            next if manager_id.zero?

            if managers[manager_id]
              managers[manager_id][Headcount::MONTHS[month]] = 1
            else
              managers[manager_id] = {Headcount::MONTHS[month] => 1}
            end
          end

          managers.each do |manager_id, months|
            headcount = user.headcounts.for_business_managers.in_years(year).where(manager_id: manager_id).first

            if headcount.present?
              headcount[:months] = months
              headcount.save
            else
              user.headcounts << Headcount.new(
                  manager_id: manager_id,
                  year: year,
                  kind: :business_manager,
                  months: months.as_json
              )
            end
          end
        end

        buser.reporting_manager_months.each do |rm|
          year = rm['Year'].to_i
          managers = {}

          ReportingManagerMonths::MONTHS.each do |month, column|
            manager_id = rm[column].to_i

            next if manager_id.zero?

            if managers[manager_id]
              managers[manager_id][Headcount::MONTHS[month]] = 1
            else
              managers[manager_id] = {Headcount::MONTHS[month] => 1}
            end
          end

          managers.each do |manager_id, months|
            headcount = user.headcounts.for_reporting_managers.in_years(year).where(manager_id: manager_id).first

            if headcount.present?
              headcount[:months] = months
              headcount.save
            else
              user.headcounts << Headcount.new(
                  manager_id: manager_id,
                  year: year,
                  kind: :reporting_manager,
                  months: months.as_json
              )
            end
          end
        end
      end
    end
  end

  class BusersRole < Business::Base
    belongs_to :buser, class_name: 'Business::Buser', foreign_key: 'buser_id'
    belongs_to :role, class_name: 'Business::Role', foreign_key: 'role_id'
  end

  class Role < Business::Base
    has_many :busers_roles, class_name: 'Business::BusersRole', foreign_key: 'role_id'
    has_many :busers, through: :busers_roles
  end

  class Dou < Business::Base
    has_many :dou_items, class_name: 'Business::DouItem', foreign_key: 'dou_id'
    has_many :item_cats, through: :dou_items

    has_many :deliverables

    scope :owned_by, ->(email) {where(DOUOwner: email)}
    # fallplan kind = [:lab, :gbs, :gts, :lbs, :snd, :gbx]
    KIND_IDS = {lab: 0, gbs: 2, gts: 3, lbs: 4, snd: 5, gbx: 6}.freeze

    def division_id(code)
      Division.find_by_code(code)&.id.nil? ? Division.find_by_code('4E').id : Division.find_by_code(code).id
    end

    def self.sync(fallplan_owner = nil)
      FallPlan.delete_all
      Deliverable.delete_all
      Outlook.delete_all
      PlannedEffort.delete_all
      puts '=== Start of DOU/Fallplan Sync ==='
      all.each do |dou|
        next if dou.EffectiveStartDate.blank?
        puts " - Create: #{dou.DOUName}"
        fallplan = FallPlan.new(
            id: dou.id,
            name: dou.DOUName,
            start_date: dou.EffectiveStartDate.in_time_zone('Asia/Shanghai').to_date,
            end_date: dou.EffectiveEndDate.in_time_zone('Asia/Shanghai').to_date,
            owner_id: User.find_by_email(dou.DOUOwner.strip)&.id,
            business_manager_id: User.by_email(dou.BusinessManager).id,
            kind: dou.dou_type_id == 1 ? KIND_IDS[:lab] : KIND_IDS[:lbs],
            dou_id: dou.DOUID,
            dou_contact: dou.DOUContact,
            # bu: dou.BU,
            division_id: dou.division_id(dou.Division),
            state: dou.State,
            pm_rate: dou.PMRate,
            billing_ratio: 1,
            other_services: [],
            lock_version: 0
        # dou_sizing: dou.TotalDOUSizing,
        # sizing: dou.TotalUpdateSizing
        )

        data = DeliverableService.get(fallplan.dou_id)
        if data.is_a?(Hash) && data.dig(:error)
          puts 'GBMS service_unavailable'
        else
          deliverables = data&.map do |deliverable|
            Deliverable.new(deliverable.to_h)
          end
          deliverables&.each do |deliverable|
            fallplan.deliverables << deliverable
          end
        end

        # comments threee.....
        #
        year_one = fallplan.year
        year_two = year_one + 1
        items = DouItem.where(dou_id: fallplan.id)&.pluck(:ItemNum)&.uniq
        items&.each do |item_num|
          year_one_cost = DouItem.find_by(dou_id: fallplan.id, Year: year_one, ItemNum: item_num)
          year_two_cost = DouItem.find_by(dou_id: fallplan.id, Year: year_two, ItemNum: item_num)
          dou_cost = Outlook.empty_cost
          fallplan_cost = Outlook.empty_cost
          if year_one_cost
            dou_cost[:year_1] = year_one_cost.DOUSizing
            fallplan_cost[:year_1] = year_one_cost.UpdateSizing
          end
          if year_two_cost
            dou_cost[:year_2] = year_two_cost.DOUSizing
            fallplan_cost[:year_2] = year_two_cost.UpdateSizing
          end
          fallplan.outlooks << Outlook.new(fallplan_id: fallplan.id, service_item_id: item_num,
                                           dou_cost: dou_cost,
                                           fallplan_cost: fallplan_cost)
        end
        planned_efforts = DouMonthRe.where(dou_id: fallplan.id)
        planned_efforts&.each do |planned_effort|
          fallplan.planned_efforts << PlannedEffort.new(fallplan_id: fallplan.id,
                                                        year: planned_effort.Year.to_i,
                                                        jan: planned_effort.JanRes.to_f,
                                                        feb: planned_effort.FebRes.to_f,
                                                        mar: planned_effort.MarRes.to_f,
                                                        apr: planned_effort.AprRes.to_f,
                                                        may: planned_effort.MayRes.to_f,
                                                        jun: planned_effort.JunRes.to_f,
                                                        jul: planned_effort.JulRes.to_f,
                                                        aug: planned_effort.AugRes.to_f,
                                                        sep: planned_effort.SepRes.to_f,
                                                        oct: planned_effort.OctRes.to_f,
                                                        nov: planned_effort.NovRes.to_f,
                                                        dec: planned_effort.DecRes.to_f)
          puts "planned effort"
        end
        puts " - Create Failed: DOU #{dou.id} Can Not Be Saved" unless fallplan.save
      end
      fallplans = fallplan_owner ? FallPlan.owned_by(fallplan_owner) : FallPlan.all
      if (deletable = fallplans.ids - Business::Dou.all.ids).present?
        deletable.each do |id|
          fallplan = FallPlan.find(id)
          if fallplan.projects.empty? && !fallplan.has_billing?
            puts " - Delete: #{fallplan.name} (#{fallplan.id})"
            fallplan.destroy
          end
        end
      end
      puts '=== End of DOU/Fallplan Sync ==='
    end
  end
# add two comments.  ....
#comment two
  class DouItem < Business::Base
    belongs_to :dou, class_name: 'Business::Dou', foreign_key: 'dou_id'
    belongs_to :item_cat, class_name: 'Business::ItemCat', foreign_key: 'ItemNum'
  end

  class ItemCat < Business::Base;
  end

  class DouMonthRe < Business::Base
    belongs_to :dou, class_name: 'Business::Dou', foreign_key: 'dou_id'
  end

  module Service
    class Deliverable < Business::Base
      belongs_to :dou
      has_many :deliverable_items, class_name: 'Business::Service::DeliverableItem', foreign_key: 'deliverable_id'
      has_many :item_cats, through: :deliverable_items
    end

    class DeliverableItem < Business::Base
      belongs_to :deliverable, class_name: 'Business::Service::Deliverable', foreign_key: 'deliverable_id'
      belongs_to :item_cat, class_name: 'Business::ItemCat', foreign_key: 'ItemNum'
    end
  end

  class Role < Business::Base;
  end

  class Site < Business::Base;
  end

  class Type < Business::Base;
  end

  class BusinessManagerMonths < Business::Base
    belongs_to :buser

    MONTHS = {
        1 => 'BM_Jan', 2 => 'BM_Feb', 3 => 'BM_Mar', 4 => 'BM_Apr', 5 => 'BM_May', 6 => 'BM_Jun',
        7 => 'BM_Jul', 8 => 'BM_Aug', 9 => 'BM_Sep', 10 => 'BM_Oct', 11 => 'BM_Nov', 12 => 'BM_Dec'
    }.freeze

    def self.current_manager_id
      where('"Year" = ?', Date.current.year.to_s).pluck(MONTHS[Date.current.month]).map(&:to_i).pop
    end
  end

  class ReportingManagerMonths < Business::Base
    belongs_to :buser

    MONTHS = {
        1 => 'PM_Jan', 2 => 'PM_Feb', 3 => 'PM_Mar', 4 => 'PM_Apr', 5 => 'PM_May', 6 => 'PM_Jun',
        7 => 'PM_Jul', 8 => 'PM_Aug', 9 => 'PM_Sep', 10 => 'PM_Oct', 11 => 'PM_Nov', 12 => 'PM_Dec'
    }.freeze

    def self.current_supervisor_id
      where('"Year" = ?', Date.current.year.to_s).pluck(MONTHS[Date.current.month]).map(&:to_i).pop
    end
  end
end
